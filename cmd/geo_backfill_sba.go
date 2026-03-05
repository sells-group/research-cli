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

var geoBackfillSBACmd = &cobra.Command{
	Use:   "backfill-sba",
	Short: "Create stub companies for SBA 7(a)/504 loan borrowers and geocode them",
	Long: `Creates stub company records for SBA 7(a) and 504 loan borrowers that don't
yet exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.sba_loans and the geo pipeline so the
scorer can use MSA-aware geo_match scoring. Borrowers are aggregated by l2locid
and ordered by most recent approval year, largest loan first.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if shouldUseTemporal(cmd) {
			return runGeoBackfillViaTemporal(ctx, cmd, "sba")
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
		skipGeocode, _ := cmd.Flags().GetBool("skip-geocode")

		log := zap.L().With(zap.String("command", "geo.backfill-sba"))

		// Build dependencies.
		cs := company.NewPostgresStore(pool)
		var gcClient geocode.Client
		var assoc *geo.Associator
		if !skipGeocode {
			gcClient = geocode.NewClient(pool,
				geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
				geocode.WithMaxRating(cfg.Geo.MaxRating),
				geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
				geocode.WithBatchConcurrency(concurrency),
			)
			if !skipMSA {
				assoc = geo.NewAssociator(pool, cs)
			}
		}

		// Find SBA loan borrowers not yet linked to a company.
		// Aggregate by l2locid (most recent approval year, largest loan first).
		rows, err := pool.Query(ctx, `
			SELECT DISTINCT ON (s.l2locid)
			       s.program, s.l2locid, s.borrname,
			       s.borrstreet, s.borrcity, s.borrstate, s.borrzip,
			       s.grossapproval
			FROM fed_data.sba_loans s
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = s.l2locid::text
				AND cm.matched_source = 'sba_loans'
			WHERE cm.id IS NULL
			  AND s.borrname IS NOT NULL AND s.borrname != ''
			ORDER BY s.l2locid, s.approvalfiscalyear DESC NULLS LAST, s.grossapproval DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-sba: query unlinked borrowers")
		}

		type sbaLoan struct {
			Program       string
			L2LocID       int64
			BorrName      string
			BorrStreet    string
			BorrCity      string
			BorrState     string
			BorrZip       string
			GrossApproval *float64
		}
		var loans []sbaLoan
		for rows.Next() {
			var l sbaLoan
			var street, city, state, zip *string
			var approval *float64
			if scanErr := rows.Scan(&l.Program, &l.L2LocID, &l.BorrName,
				&street, &city, &state, &zip, &approval); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-sba: scan loan")
			}
			if street != nil {
				l.BorrStreet = *street
			}
			if city != nil {
				l.BorrCity = *city
			}
			if state != nil {
				l.BorrState = *state
			}
			if zip != nil {
				l.BorrZip = *zip
			}
			l.GrossApproval = approval
			loans = append(loans, l)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-sba: iterate loans")
		}

		if len(loans) == 0 {
			fmt.Println("No unlinked SBA loan borrowers found")
			return nil
		}

		log.Info("starting SBA backfill",
			zap.Int("unlinked_borrowers", len(loans)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, failed int

		// Process in batches.
		for i := 0; i < len(loans); i += batchSize {
			end := i + batchSize
			if end > len(loans) {
				end = len(loans)
			}
			batch := loans[i:end]

			for _, l := range batch {
				locIDStr := fmt.Sprintf("%d", l.L2LocID)

				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    l.BorrName,
					City:    l.BorrCity,
					State:   l.BorrState,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.Int64("l2locid", l.L2LocID),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Upsert SBA loan identifier.
				ident := &company.Identifier{
					CompanyID:  cr.ID,
					System:     company.SystemSBALoan,
					Identifier: locIDStr,
				}
				if identErr := cs.UpsertIdentifier(ctx, ident); identErr != nil {
					log.Warn("failed to upsert identifier",
						zap.Int64("l2locid", l.L2LocID),
						zap.Error(identErr),
					)
				}

				// 3. Upsert address.
				street := strings.TrimSpace(l.BorrStreet)
				conf := 1.0
				addr := &company.Address{
					CompanyID:   cr.ID,
					AddressType: company.AddressPrincipal,
					Street:      street,
					City:        l.BorrCity,
					State:       l.BorrState,
					ZipCode:     l.BorrZip,
					Country:     "US",
					IsPrimary:   true,
					Source:      "sba_loans",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.Int64("l2locid", l.L2LocID),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 4. Geocode the address (skip if --skip-geocode).
				if gcClient != nil {
					gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
						ID:      fmt.Sprintf("%d", addr.ID),
						Street:  street,
						City:    l.BorrCity,
						State:   l.BorrState,
						ZipCode: l.BorrZip,
					})
					if gcErr != nil {
						log.Debug("geocode failed",
							zap.Int64("l2locid", l.L2LocID),
							zap.Error(gcErr),
						)
					} else if gcResult.Matched {
						if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
							log.Warn("failed to update geocode",
								zap.Int64("l2locid", l.L2LocID),
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
										zap.Int64("l2locid", l.L2LocID),
										zap.Error(assocErr),
									)
								} else {
									msaCount += len(relations)
								}
							}
						}
					}
				}

				// 6. Link via company_matches.
				matchConf := 1.0
				match := &company.Match{
					CompanyID:     cr.ID,
					MatchedSource: "sba_loans",
					MatchedKey:    locIDStr,
					MatchType:     "direct_sba_loan",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.Int64("l2locid", l.L2LocID),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(loans)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
			)
		}

		fmt.Printf("SBA backfill complete: %d created, %d geocoded, %d linked, %d MSA associations, %d failed\n",
			created, geocoded, linked, msaCount, failed)
		return nil
	},
}

func init() {
	f := geoBackfillSBACmd.Flags()
	f.Int("limit", 10000, "maximum number of SBA loan borrowers to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-geocode", false, "skip geocoding step")
	f.Bool("skip-msa", false, "skip MSA association step")
	addDirectFlag(geoBackfillSBACmd)
	geoCmd.AddCommand(geoBackfillSBACmd)
}
