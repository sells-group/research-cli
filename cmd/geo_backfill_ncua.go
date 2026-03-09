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

var geoBackfillNCUACmd = &cobra.Command{
	Use:   "backfill-ncua",
	Short: "Create stub companies for NCUA credit unions and geocode them",
	Long: `Creates stub company records for NCUA credit unions that don't yet exist in
public.companies, geocodes their addresses via PostGIS TIGER, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.ncua_call_reports and the geo pipeline
so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if useTemporal, _ := cmd.Flags().GetBool("temporal"); useTemporal {
			return runGeoBackfillViaTemporal(ctx, cmd, "ncua")
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

		log := zap.L().With(zap.String("command", "geo.backfill-ncua"))

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

		// Find NCUA credit unions not yet linked to a company.
		// Aggregate by cu_number, pick the latest cycle_date, order by total_assets DESC.
		rows, err := pool.Query(ctx, `
			SELECT DISTINCT ON (n.cu_number) n.cu_number, n.cu_name,
			       n.street, n.city, n.state, n.zip_code
			FROM fed_data.ncua_call_reports n
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = n.cu_number::text
				AND cm.matched_source = 'ncua_call_reports'
			WHERE cm.id IS NULL
			ORDER BY n.cu_number, n.cycle_date DESC, n.total_assets DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-ncua: query unlinked credit unions")
		}

		type ncuaCU struct {
			CUNumber int
			Name     string
			Street   string
			City     string
			State    string
			Zip      string
		}
		var creditUnions []ncuaCU
		for rows.Next() {
			var cu ncuaCU
			var street, city, state, zip *string
			if scanErr := rows.Scan(&cu.CUNumber, &cu.Name, &street, &city, &state, &zip); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-ncua: scan credit union")
			}
			if street != nil {
				cu.Street = *street
			}
			if city != nil {
				cu.City = *city
			}
			if state != nil {
				cu.State = *state
			}
			if zip != nil {
				cu.Zip = *zip
			}
			creditUnions = append(creditUnions, cu)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-ncua: iterate credit unions")
		}

		if len(creditUnions) == 0 {
			fmt.Println("No unlinked NCUA credit unions found")
			return nil
		}

		log.Info("starting NCUA credit union backfill",
			zap.Int("unlinked_credit_unions", len(creditUnions)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		for i := 0; i < len(creditUnions); i += batchSize {
			end := i + batchSize
			if end > len(creditUnions) {
				end = len(creditUnions)
			}
			batch := creditUnions[i:end]

			for _, cu := range batch {
				cuStr := fmt.Sprintf("%d", cu.CUNumber)

				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    cu.Name,
					City:    cu.City,
					State:   cu.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.Int("cu_number", cu.CUNumber),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Upsert NCUA identifier.
				if idErr := cs.UpsertIdentifier(ctx, &company.Identifier{
					CompanyID:  cr.ID,
					System:     company.SystemNCUA,
					Identifier: cuStr,
				}); idErr != nil {
					log.Warn("failed to upsert NCUA identifier",
						zap.Int("cu_number", cu.CUNumber),
						zap.Error(idErr),
					)
				}

				// 3. Create address.
				street := strings.TrimSpace(cu.Street)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressPrincipal,
					Street:      street,
					City:        cu.City,
					State:       cu.State,
					ZipCode:     cu.Zip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "ncua_call_reports",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.Int("cu_number", cu.CUNumber),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 4. Geocode via TIGER.
				gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
					ID:      fmt.Sprintf("%d", addr.ID),
					Street:  street,
					City:    cu.City,
					State:   cu.State,
					ZipCode: cu.Zip,
				})
				if gcErr != nil {
					log.Debug("geocode failed",
						zap.Int("cu_number", cu.CUNumber),
						zap.Error(gcErr),
					)
				} else if gcResult.Matched {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
						log.Warn("failed to update geocode",
							zap.Int("cu_number", cu.CUNumber),
							zap.Error(updateErr),
						)
					} else {
						geocoded++

						if assoc != nil {
							topN := cfg.Geo.TopMSAs
							if topN <= 0 {
								topN = 3
							}
							relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, topN)
							if assocErr != nil {
								log.Warn("MSA association failed",
									zap.Int("cu_number", cu.CUNumber),
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
					MatchedSource: "ncua_call_reports",
					MatchedKey:    cuStr,
					MatchType:     "direct_ncua_charter",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.Int("cu_number", cu.CUNumber),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(creditUnions)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("NCUA backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfillNCUACmd.Flags()
	f.Int("limit", 10000, "maximum number of credit unions to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfillNCUACmd)
}
