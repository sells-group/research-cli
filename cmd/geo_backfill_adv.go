package main

import (
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

var geoBackfillADVCmd = &cobra.Command{
	Use:   "backfill-adv",
	Short: "Create stub companies for ADV firms and geocode them",
	Long: `Creates stub company records for ADV firms that don't yet exist in
public.companies, geocodes their addresses via PostGIS TIGER, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.adv_firms and the geo pipeline so
the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		limit, _ := cmd.Flags().GetInt("limit")
		batchSize, _ := cmd.Flags().GetInt("batch-size")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		skipMSA, _ := cmd.Flags().GetBool("skip-msa")

		log := zap.L().With(zap.String("command", "geo.backfill-adv"))

		// Build dependencies.
		gcClient := geocode.NewClient(pool,
			geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
			geocode.WithMaxRating(cfg.Geo.MaxRating),
			geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
			geocode.WithBatchConcurrency(concurrency),
		)
		cs := company.NewPostgresStore(pool)
		var assoc *geo.Associator
		if !skipMSA {
			assoc = geo.NewAssociator(pool, cs)
		}

		// Find ADV firms not yet linked to a company.
		rows, err := pool.Query(ctx, `
			SELECT af.crd_number, af.firm_name,
			       af.street1, af.street2, af.city, af.state, af.zip, af.website
			FROM fed_data.adv_firms af
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = af.crd_number::text
				AND cm.matched_source = 'adv_firms'
			WHERE cm.id IS NULL
			ORDER BY af.aum DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-adv: query unlinked firms")
		}

		type advFirm struct {
			CRDNumber int
			FirmName  string
			Street1   string
			Street2   string
			City      string
			State     string
			Zip       string
			Website   string
		}
		var firms []advFirm
		for rows.Next() {
			var f advFirm
			var street1, street2, city, state, zip, website *string
			if scanErr := rows.Scan(&f.CRDNumber, &f.FirmName, &street1, &street2, &city, &state, &zip, &website); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-adv: scan firm")
			}
			if street1 != nil {
				f.Street1 = *street1
			}
			if street2 != nil {
				f.Street2 = *street2
			}
			if city != nil {
				f.City = *city
			}
			if state != nil {
				f.State = *state
			}
			if zip != nil {
				f.Zip = *zip
			}
			if website != nil {
				f.Website = *website
			}
			firms = append(firms, f)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-adv: iterate firms")
		}

		if len(firms) == 0 {
			fmt.Println("No unlinked ADV firms found")
			return nil
		}

		log.Info("starting ADV firm backfill",
			zap.Int("unlinked_firms", len(firms)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		// Process in batches.
		for i := 0; i < len(firms); i += batchSize {
			end := i + batchSize
			if end > len(firms) {
				end = len(firms)
			}
			batch := firms[i:end]

			for _, f := range batch {
				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    f.FirmName,
					Domain:  f.Website,
					Website: f.Website,
					City:    f.City,
					State:   f.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.Int("crd_number", f.CRDNumber),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Create primary address.
				street := strings.TrimSpace(f.Street1 + " " + f.Street2)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressPrincipal,
					Street:      street,
					City:        f.City,
					State:       f.State,
					ZipCode:     f.Zip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "adv_firms",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.Int("crd_number", f.CRDNumber),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 3. Geocode the address.
				gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
					ID:      fmt.Sprintf("%d", addr.ID),
					Street:  street,
					City:    f.City,
					State:   f.State,
					ZipCode: f.Zip,
				})
				if gcErr != nil {
					log.Debug("geocode failed",
						zap.Int("crd_number", f.CRDNumber),
						zap.Error(gcErr),
					)
				} else if gcResult.Matched {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
						log.Warn("failed to update geocode",
							zap.Int("crd_number", f.CRDNumber),
							zap.Error(updateErr),
						)
					} else {
						geocoded++

						// 4. Associate with MSAs.
						if assoc != nil {
							topN := cfg.Geo.TopMSAs
							if topN <= 0 {
								topN = 3
							}
							relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, topN)
							if assocErr != nil {
								log.Warn("MSA association failed",
									zap.Int("crd_number", f.CRDNumber),
									zap.Error(assocErr),
								)
							} else {
								msaCount += len(relations)
							}
						}
					}
				}

				// 5. Link via company_matches.
				matchConf := 1.0
				match := &company.Match{
					CompanyID:     cr.ID,
					MatchedSource: "adv_firms",
					MatchedKey:    fmt.Sprintf("%d", f.CRDNumber),
					MatchType:     "direct_crd",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.Int("crd_number", f.CRDNumber),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(firms)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("ADV backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfillADVCmd.Flags()
	f.Int("limit", 1000, "maximum number of ADV firms to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillADVCmd)
}
