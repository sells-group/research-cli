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

var geoBackfillUsaspendingCmd = &cobra.Command{
	Use:   "backfill-usaspending",
	Short: "Create stub companies for USAspending recipients and geocode them",
	Long: `Creates stub company records for USAspending award recipients that don't yet
exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.usaspending_awards and the geo pipeline
so the scorer can use MSA-aware geo_match scoring.`,
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

		log := zap.L().With(zap.String("command", "geo.backfill-usaspending"))

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

		// Find USAspending recipients not yet linked to a company.
		rows, err := pool.Query(ctx, `
			WITH recipients AS (
				SELECT DISTINCT ON (recipient_uei)
					recipient_uei AS uei, recipient_duns AS duns,
					recipient_name AS name,
					recipient_address_line_1 AS street,
					recipient_city AS city, recipient_state AS state,
					recipient_zip AS zip, recipient_country AS country,
					SUM(total_obligated_amount) OVER (PARTITION BY recipient_uei) AS total_obligated
				FROM fed_data.usaspending_awards
				WHERE recipient_uei IS NOT NULL AND recipient_uei != ''
					AND recipient_country = 'USA'
				ORDER BY recipient_uei, award_latest_action_date DESC
			)
			SELECT r.*
			FROM recipients r
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = r.uei AND cm.matched_source = 'usaspending_awards'
			WHERE cm.id IS NULL
			ORDER BY r.total_obligated DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-usaspending: query unlinked recipients")
		}

		type usaspendingRecipient struct {
			UEI            string
			DUNS           string
			Name           string
			Street         string
			City           string
			State          string
			Zip            string
			Country        string
			TotalObligated *float64
		}
		var recipients []usaspendingRecipient
		for rows.Next() {
			var r usaspendingRecipient
			var uei, duns, name, street, city, state, zip, country *string
			var totalObligated *float64
			if scanErr := rows.Scan(&uei, &duns, &name, &street, &city, &state, &zip, &country, &totalObligated); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-usaspending: scan recipient")
			}
			if uei != nil {
				r.UEI = *uei
			}
			if duns != nil {
				r.DUNS = *duns
			}
			if name != nil {
				r.Name = *name
			}
			if street != nil {
				r.Street = *street
			}
			if city != nil {
				r.City = *city
			}
			if state != nil {
				r.State = *state
			}
			if zip != nil {
				r.Zip = *zip
			}
			if country != nil {
				r.Country = *country
			}
			r.TotalObligated = totalObligated
			recipients = append(recipients, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-usaspending: iterate recipients")
		}

		if len(recipients) == 0 {
			fmt.Println("No unlinked USAspending recipients found")
			return nil
		}

		log.Info("starting USAspending recipient backfill",
			zap.Int("unlinked_recipients", len(recipients)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		// Process in batches.
		for i := 0; i < len(recipients); i += batchSize {
			end := i + batchSize
			if end > len(recipients) {
				end = len(recipients)
			}
			batch := recipients[i:end]

			for _, r := range batch {
				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    r.Name,
					City:    r.City,
					State:   r.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.String("uei", r.UEI),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Upsert UEI identifier.
				if upsertErr := cs.UpsertIdentifier(ctx, &company.Identifier{
					CompanyID:  cr.ID,
					System:     company.SystemUEI,
					Identifier: r.UEI,
				}); upsertErr != nil {
					log.Warn("failed to upsert UEI identifier",
						zap.String("uei", r.UEI),
						zap.Error(upsertErr),
					)
				}

				// 3. Upsert DUNS identifier if present (legacy linkage).
				if r.DUNS != "" {
					if upsertErr := cs.UpsertIdentifier(ctx, &company.Identifier{
						CompanyID:  cr.ID,
						System:     company.SystemDUNS,
						Identifier: r.DUNS,
					}); upsertErr != nil {
						log.Warn("failed to upsert DUNS identifier",
							zap.String("uei", r.UEI),
							zap.String("duns", r.DUNS),
							zap.Error(upsertErr),
						)
					}
				}

				// 4. Create mailing address.
				street := strings.TrimSpace(r.Street)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressMailing,
					Street:      street,
					City:        r.City,
					State:       r.State,
					ZipCode:     r.Zip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "usaspending_awards",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.String("uei", r.UEI),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 5. Geocode the address.
				gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
					ID:      fmt.Sprintf("%d", addr.ID),
					Street:  street,
					City:    r.City,
					State:   r.State,
					ZipCode: r.Zip,
				})
				if gcErr != nil {
					log.Debug("geocode failed",
						zap.String("uei", r.UEI),
						zap.Error(gcErr),
					)
				} else if gcResult.Matched {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
						log.Warn("failed to update geocode",
							zap.String("uei", r.UEI),
							zap.Error(updateErr),
						)
					} else {
						geocoded++

						// 6. Associate with MSAs.
						if assoc != nil {
							topN := cfg.Geo.TopMSAs
							if topN <= 0 {
								topN = 3
							}
							relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, topN)
							if assocErr != nil {
								log.Warn("MSA association failed",
									zap.String("uei", r.UEI),
									zap.Error(assocErr),
								)
							} else {
								msaCount += len(relations)
							}
						}
					}
				}

				// 7. Link via company_matches.
				matchConf := 1.0
				match := &company.Match{
					CompanyID:     cr.ID,
					MatchedSource: "usaspending_awards",
					MatchedKey:    r.UEI,
					MatchType:     "direct_uei",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.String("uei", r.UEI),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(recipients)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("USAspending backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfillUsaspendingCmd.Flags()
	f.Int("limit", 10000, "maximum number of USAspending recipients to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillUsaspendingCmd)
}
