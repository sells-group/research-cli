package main

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

var geoBackfillFDICCmd = &cobra.Command{
	Use:   "backfill-fdic",
	Short: "Create stub companies for FDIC institutions and geocode them",
	Long: `Creates stub company records for FDIC institutions that don't yet exist in
public.companies, geocodes their addresses (using FDIC-provided lat/lng when
available, PostGIS TIGER otherwise), creates branch sub-locations, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.fdic_institutions and the geo pipeline
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
		skipBranches, _ := cmd.Flags().GetBool("skip-branches")

		log := zap.L().With(zap.String("command", "geo.backfill-fdic"))

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

		// Find FDIC institutions not yet linked to a company.
		rows, err := pool.Query(ctx, `
			SELECT fi.cert, fi.name,
			       fi.address, fi.city, fi.stalp, fi.zip, fi.webaddr,
			       fi.latitude, fi.longitude, fi.asset
			FROM fed_data.fdic_institutions fi
			LEFT JOIN public.company_matches cm
				ON cm.matched_key = fi.cert::text
				AND cm.matched_source = 'fdic_institutions'
			WHERE cm.id IS NULL AND fi.active = 1
			ORDER BY fi.asset DESC NULLS LAST
			LIMIT $1`, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill-fdic: query unlinked institutions")
		}

		type fdicInst struct {
			Cert      int
			Name      string
			Address   string
			City      string
			State     string
			Zip       string
			Website   string
			Latitude  *float64
			Longitude *float64
			Asset     *int64
		}
		var institutions []fdicInst
		for rows.Next() {
			var f fdicInst
			var addr, city, state, zip, website *string
			var lat, lng *float64
			var asset *int64
			if scanErr := rows.Scan(&f.Cert, &f.Name, &addr, &city, &state, &zip, &website, &lat, &lng, &asset); scanErr != nil {
				rows.Close()
				return eris.Wrap(scanErr, "geo backfill-fdic: scan institution")
			}
			if addr != nil {
				f.Address = *addr
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
			f.Latitude = lat
			f.Longitude = lng
			f.Asset = asset
			institutions = append(institutions, f)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo backfill-fdic: iterate institutions")
		}

		if len(institutions) == 0 {
			fmt.Println("No unlinked FDIC institutions found")
			return nil
		}

		log.Info("starting FDIC institution backfill",
			zap.Int("unlinked_institutions", len(institutions)),
			zap.Int("batch_size", batchSize),
			zap.Int("concurrency", concurrency),
		)

		var created, geocoded, linked, msaCount, branchCount, failed int

		// Process in batches.
		for i := 0; i < len(institutions); i += batchSize {
			end := i + batchSize
			if end > len(institutions) {
				end = len(institutions)
			}
			batch := institutions[i:end]

			for _, f := range batch {
				certStr := fmt.Sprintf("%d", f.Cert)

				// 1. Create stub company.
				cr := &company.CompanyRecord{
					Name:    f.Name,
					Domain:  f.Website,
					Website: f.Website,
					City:    f.City,
					State:   f.State,
					Country: "US",
				}
				if createErr := cs.CreateCompany(ctx, cr); createErr != nil {
					log.Warn("failed to create stub company",
						zap.Int("cert", f.Cert),
						zap.Error(createErr),
					)
					failed++
					continue
				}
				created++

				// 2. Upsert FDIC identifier.
				if idErr := cs.UpsertIdentifier(ctx, &company.Identifier{
					CompanyID:  cr.ID,
					System:     company.SystemFDIC,
					Identifier: certStr,
				}); idErr != nil {
					log.Warn("failed to upsert FDIC identifier",
						zap.Int("cert", f.Cert),
						zap.Error(idErr),
					)
				}

				// 3. Create principal address.
				street := strings.TrimSpace(f.Address)
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
					Source:      "fdic_institutions",
					Confidence:  &conf,
				}
				if upsertErr := cs.UpsertAddress(ctx, addr); upsertErr != nil {
					log.Warn("failed to create address",
						zap.Int("cert", f.Cert),
						zap.Error(upsertErr),
					)
					failed++
					continue
				}

				// 4. Geocode: use FDIC lat/lng if available, else TIGER.
				if f.Latitude != nil && f.Longitude != nil && *f.Latitude != 0 && *f.Longitude != 0 {
					if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, *f.Latitude, *f.Longitude, "fdic", "provider", ""); updateErr != nil {
						log.Warn("failed to update geocode from FDIC",
							zap.Int("cert", f.Cert),
							zap.Error(updateErr),
						)
					} else {
						geocoded++

						if assoc != nil {
							topN := cfg.Geo.TopMSAs
							if topN <= 0 {
								topN = 3
							}
							relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, *f.Latitude, *f.Longitude, topN)
							if assocErr != nil {
								log.Warn("MSA association failed",
									zap.Int("cert", f.Cert),
									zap.Error(assocErr),
								)
							} else {
								msaCount += len(relations)
							}
						}
					}
				} else {
					gcResult, gcErr := gcClient.Geocode(ctx, geocode.AddressInput{
						ID:      fmt.Sprintf("%d", addr.ID),
						Street:  street,
						City:    f.City,
						State:   f.State,
						ZipCode: f.Zip,
					})
					if gcErr != nil {
						log.Debug("geocode failed",
							zap.Int("cert", f.Cert),
							zap.Error(gcErr),
						)
					} else if gcResult.Matched {
						if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); updateErr != nil {
							log.Warn("failed to update geocode",
								zap.Int("cert", f.Cert),
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
										zap.Int("cert", f.Cert),
										zap.Error(assocErr),
									)
								} else {
									msaCount += len(relations)
								}
							}
						}
					}
				}

				// 5. Create branch addresses (if not skipped).
				if !skipBranches {
					topN := cfg.Geo.TopMSAs
					if topN <= 0 {
						topN = 3
					}
					bc, bmc, branchErr := backfillFDICBranches(ctx, pool, cs, assoc, f.Cert, cr.ID, topN)
					if branchErr != nil {
						log.Warn("branch backfill failed",
							zap.Int("cert", f.Cert),
							zap.Error(branchErr),
						)
					} else {
						branchCount += bc
						msaCount += bmc
					}
				}

				// 6. Link via company_matches.
				matchConf := 1.0
				match := &company.Match{
					CompanyID:     cr.ID,
					MatchedSource: "fdic_institutions",
					MatchedKey:    certStr,
					MatchType:     "direct_fdic_cert",
					Confidence:    &matchConf,
				}
				if matchErr := cs.UpsertMatch(ctx, match); matchErr != nil {
					log.Warn("failed to create match",
						zap.Int("cert", f.Cert),
						zap.Error(matchErr),
					)
				} else {
					linked++
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(institutions)),
				zap.Int("created", created),
				zap.Int("geocoded", geocoded),
				zap.Int("linked", linked),
				zap.Int("branches", branchCount),
			)
		}

		fmt.Printf("FDIC backfill complete: %d created, %d geocoded, %d linked, %d branches, %d MSA associations, %d failed\n",
			created, geocoded, linked, branchCount, msaCount, failed)
		return nil
	},
}

// backfillFDICBranches creates branch addresses for the given FDIC institution.
func backfillFDICBranches(ctx context.Context, pool *pgxpool.Pool, cs *company.PostgresStore, assoc *geo.Associator, cert int, companyID int64, topMSAs int) (int, int, error) {
	branchRows, err := pool.Query(ctx, `
		SELECT uni_num, off_name, address, city, stalp, zip, county,
		       latitude, longitude, main_off, stcnty
		FROM fed_data.fdic_branches WHERE cert = $1`, cert)
	if err != nil {
		return 0, 0, eris.Wrapf(err, "backfill-fdic: query branches for cert %d", cert)
	}
	defer branchRows.Close()

	log := zap.L()
	var branchCount, msaCount int

	for branchRows.Next() {
		var uniNum, mainOff *int
		var offName, addr, city, state, zip, county, stcnty *string
		var lat, lng *float64
		if scanErr := branchRows.Scan(&uniNum, &offName, &addr, &city, &state, &zip, &county, &lat, &lng, &mainOff, &stcnty); scanErr != nil {
			return branchCount, msaCount, eris.Wrap(scanErr, "backfill-fdic: scan branch")
		}

		branchAddr := &company.Address{
			CompanyID:   companyID,
			AddressType: company.AddressBranch,
			IsPrimary:   false,
			Source:      "fdic_branches",
		}
		if offName != nil {
			branchAddr.Street = strings.TrimSpace(*offName)
		}
		if addr != nil {
			branchAddr.Street = strings.TrimSpace(*addr)
		}
		if city != nil {
			branchAddr.City = *city
		}
		if state != nil {
			branchAddr.State = *state
		}
		if zip != nil {
			branchAddr.ZipCode = *zip
		}
		if stcnty != nil {
			branchAddr.CountyFIPS = *stcnty
		}
		branchAddr.Country = "US"

		if upsertErr := cs.UpsertAddress(ctx, branchAddr); upsertErr != nil {
			log.Debug("failed to create branch address",
				zap.Int("cert", cert),
				zap.Error(upsertErr),
			)
			continue
		}
		branchCount++

		// Use FDIC-provided coordinates if available.
		if lat != nil && lng != nil && *lat != 0 && *lng != 0 {
			if updateErr := cs.UpdateAddressGeocode(ctx, branchAddr.ID, *lat, *lng, "fdic", "provider", ""); updateErr != nil {
				log.Debug("failed to update branch geocode",
					zap.Int("cert", cert),
					zap.Error(updateErr),
				)
			} else if assoc != nil {
				relations, assocErr := assoc.AssociateAddress(ctx, branchAddr.ID, *lat, *lng, topMSAs)
				if assocErr != nil {
					log.Debug("branch MSA association failed",
						zap.Int("cert", cert),
						zap.Error(assocErr),
					)
				} else {
					msaCount += len(relations)
				}
			}
		}
	}

	return branchCount, msaCount, branchRows.Err()
}

func init() {
	f := geoBackfillFDICCmd.Flags()
	f.Int("limit", 1000, "maximum number of FDIC institutions to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("skip-branches", false, "skip branch location creation")
	geoCmd.AddCommand(geoBackfillFDICCmd)
}
