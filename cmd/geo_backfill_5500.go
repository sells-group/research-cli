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

var geoBackfill5500Cmd = &cobra.Command{
	Use:   "backfill-5500",
	Short: "Create stub companies for Form 5500 plan sponsors and geocode them",
	Long: `Creates stub company records for Form 5500 plan sponsors that don't yet
exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.form_5500 / form_5500_sf and the
geo pipeline so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if useTemporal, _ := cmd.Flags().GetBool("temporal"); useTemporal {
			return runGeoBackfillViaTemporal(ctx, cmd, "5500")
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

		log := zap.L().With(zap.String("command", "geo.backfill-5500"))

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

		// Find Form 5500 sponsors not yet linked to a company.
		rows, err := pool.Query(ctx, `
			WITH sponsors AS (
				SELECT DISTINCT ON (spons_dfe_ein)
					spons_dfe_ein AS ein, sponsor_dfe_name AS name,
					spons_dfe_mail_us_address1 AS street1, spons_dfe_mail_us_address2 AS street2,
					spons_dfe_mail_us_city AS city, spons_dfe_mail_us_state AS state,
					spons_dfe_mail_us_zip AS zip
				FROM fed_data.form_5500
				WHERE spons_dfe_ein IS NOT NULL AND spons_dfe_ein != ''
				ORDER BY spons_dfe_ein, form_plan_year_begin_date DESC
				UNION ALL
				SELECT DISTINCT ON (sf_spons_ein)
					sf_spons_ein, sf_sponsor_name,
					sf_spons_us_address1, sf_spons_us_address2,
					sf_spons_us_city, sf_spons_us_state, sf_spons_us_zip
				FROM fed_data.form_5500_sf
				WHERE sf_spons_ein IS NOT NULL AND sf_spons_ein != ''
				ORDER BY sf_spons_ein, sf_plan_year_begin_date DESC
			)
			SELECT DISTINCT ON (s.ein) s.*
			FROM sponsors s
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = s.ein AND cm.matched_source = 'form_5500'
			WHERE cm.id IS NULL
			ORDER BY s.ein
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-5500: query unlinked sponsors")
		}

		type form5500Sponsor struct {
			EIN     string
			Name    string
			Street1 string
			Street2 string
			City    string
			State   string
			Zip     string
		}
		var sponsors []form5500Sponsor
		for rows.Next() {
			var f form5500Sponsor
			var ein, name, street1, street2, city, state, zip *string
			if scanErr := rows.Scan(&ein, &name, &street1, &street2, &city, &state, &zip); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-5500: scan sponsor")
			}
			if ein != nil {
				f.EIN = *ein
			}
			if name != nil {
				f.Name = *name
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
			sponsors = append(sponsors, f)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-5500: iterate sponsors")
		}

		if len(sponsors) == 0 {
			fmt.Println("No unlinked Form 5500 sponsors found")
			return nil
		}

		log.Info("starting Form 5500 sponsor backfill",
			zap.Int("unlinked_sponsors", len(sponsors)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		// Process in batches.
		for i := 0; i < len(sponsors); i += batchSize {
			end := i + batchSize
			if end > len(sponsors) {
				end = len(sponsors)
			}
			batch := sponsors[i:end]

			for _, f := range batch {
				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    f.Name,
					City:    f.City,
					State:   f.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.String("ein", f.EIN),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Create mailing address.
				street := strings.TrimSpace(f.Street1 + " " + f.Street2)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressMailing,
					Street:      street,
					City:        f.City,
					State:       f.State,
					ZipCode:     f.Zip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "form_5500",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.String("ein", f.EIN),
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
						zap.String("ein", f.EIN),
						zap.Error(gcErr),
					)
				} else if gcResult.Matched {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
						log.Warn("failed to update geocode",
							zap.String("ein", f.EIN),
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
									zap.String("ein", f.EIN),
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
					MatchedSource: "form_5500",
					MatchedKey:    f.EIN,
					MatchType:     "direct_ein",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.String("ein", f.EIN),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(sponsors)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("Form 5500 backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfill5500Cmd.Flags()
	f.Int("limit", 10000, "maximum number of Form 5500 sponsors to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfill5500Cmd)
}
