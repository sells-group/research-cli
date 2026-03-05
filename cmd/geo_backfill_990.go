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

var geoBackfill990Cmd = &cobra.Command{
	Use:   "backfill-990",
	Short: "Create stub companies for IRS EO BMF organizations and geocode them",
	Long: `Creates stub company records for IRS Exempt Organization BMF entries that
don't yet exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.eo_bmf and the geo pipeline so the
scorer can use MSA-aware geo_match scoring. Organizations are processed in
descending asset order so the largest nonprofits/foundations are linked first.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if useTemporal, _ := cmd.Flags().GetBool("temporal"); useTemporal {
			return runGeoBackfillViaTemporal(ctx, cmd, "990")
		}

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		limit, _ := cmd.Flags().GetInt("limit")
		batchSize, _ := cmd.Flags().GetInt("batch-size")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		skipMSA, _ := cmd.Flags().GetBool("skip-msa")

		log := zap.L().With(zap.String("command", "geo.backfill-990"))

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

		// Find EO BMF orgs not yet linked to a company, prioritized by asset size.
		rows, err := pool.Query(ctx, `
			SELECT ein, name, street, city, state, zip
			FROM fed_data.eo_bmf
			WHERE ein IS NOT NULL AND status = 1
				AND ein NOT IN (
					SELECT matched_key FROM public.company_matches
					WHERE matched_source = 'eo_bmf'
				)
			ORDER BY asset_amt DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-990: query unlinked orgs")
		}

		type eoBMFOrg struct {
			EIN    string
			Name   string
			Street string
			City   string
			State  string
			Zip    string
		}
		var orgs []eoBMFOrg
		for rows.Next() {
			var o eoBMFOrg
			var ein, name, street, city, state, zip *string
			if scanErr := rows.Scan(&ein, &name, &street, &city, &state, &zip); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-990: scan org")
			}
			if ein != nil {
				o.EIN = *ein
			}
			if name != nil {
				o.Name = *name
			}
			if street != nil {
				o.Street = *street
			}
			if city != nil {
				o.City = *city
			}
			if state != nil {
				o.State = *state
			}
			if zip != nil {
				o.Zip = *zip
			}
			orgs = append(orgs, o)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-990: iterate orgs")
		}

		if len(orgs) == 0 {
			fmt.Println("No unlinked EO BMF organizations found")
			return nil
		}

		log.Info("starting EO BMF backfill",
			zap.Int("unlinked_orgs", len(orgs)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		// Process in batches.
		for i := 0; i < len(orgs); i += batchSize {
			end := i + batchSize
			if end > len(orgs) {
				end = len(orgs)
			}
			batch := orgs[i:end]

			for _, o := range batch {
				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    o.Name,
					City:    o.City,
					State:   o.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.String("ein", o.EIN),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Upsert EIN identifier.
				ident := &company.Identifier{
					CompanyID:  cr.ID,
					System:     company.SystemEIN,
					Identifier: o.EIN,
				}
				if identErr := cs.UpsertIdentifier(ctx, ident); identErr != nil {
					log.Warn("failed to upsert identifier",
						zap.String("ein", o.EIN),
						zap.Error(identErr),
					)
				}

				// 3. Upsert address.
				street := strings.TrimSpace(o.Street)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressMailing,
					Street:      street,
					City:        o.City,
					State:       o.State,
					ZipCode:     o.Zip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "eo_bmf",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.String("ein", o.EIN),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 4. Geocode the address.
				gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
					ID:      fmt.Sprintf("%d", addr.ID),
					Street:  street,
					City:    o.City,
					State:   o.State,
					ZipCode: o.Zip,
				})
				if gcErr != nil {
					log.Debug("geocode failed",
						zap.String("ein", o.EIN),
						zap.Error(gcErr),
					)
				} else if gcResult.Matched {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
						log.Warn("failed to update geocode",
							zap.String("ein", o.EIN),
							zap.Error(updateErr),
						)
					} else {
						geocoded++

						// 5. Associate with MSAs.
						if assoc != nil {
							topN := cfg.Geo.TopMSAs
							if topN <= 0 {
								topN = 3
							}
							relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, topN)
							if assocErr != nil {
								log.Warn("MSA association failed",
									zap.String("ein", o.EIN),
									zap.Error(assocErr),
								)
							} else {
								msaCount += len(relations)
							}
						}
					}
				}

				// 6. Link via company_matches.
				matchConf := 1.0
				match := &company.Match{
					CompanyID:     cr.ID,
					MatchedSource: "eo_bmf",
					MatchedKey:    o.EIN,
					MatchType:     "direct_ein",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.String("ein", o.EIN),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(orgs)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("EO BMF backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfill990Cmd.Flags()
	f.Int("limit", 10000, "maximum number of EO BMF organizations to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfill990Cmd)
}
