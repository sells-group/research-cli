package main

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

var geoBackfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Geocode ungeocoded addresses",
	Long:  "Geocodes existing ungeocoded addresses using PostGIS tiger geocoder and optionally associates them with MSAs.",
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
		skipMSA, _ := cmd.Flags().GetBool("skip-msa")

		log := zap.L().With(zap.String("command", "geo.backfill"))

		// Build PostGIS geocode client.
		gcClient := geocode.NewClient(pool,
			geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
			geocode.WithMaxRating(cfg.Geo.MaxRating),
		)

		// Build company store and geo associator.
		cs := company.NewPostgresStore(pool)
		var assoc *geo.Associator
		if !skipMSA {
			assoc = geo.NewAssociator(pool, cs)
		}

		// Fetch ungeocoded addresses.
		addrs, err := cs.GetUngeocodedAddresses(ctx, limit)
		if err != nil {
			return eris.Wrap(err, "geo backfill: get ungeocoded addresses")
		}

		if len(addrs) == 0 {
			fmt.Println("No ungeocoded addresses found")
			return nil
		}

		log.Info("starting geocode backfill",
			zap.Int("addresses", len(addrs)),
			zap.Int("batch_size", batchSize),
			zap.Bool("skip_msa", skipMSA),
		)

		var geocoded, failed, msaCount int

		// Process in batches.
		for i := 0; i < len(addrs); i += batchSize {
			end := i + batchSize
			if end > len(addrs) {
				end = len(addrs)
			}
			batch := addrs[i:end]

			// Build batch input.
			inputs := make([]geocode.AddressInput, len(batch))
			for j, addr := range batch {
				inputs[j] = geocode.AddressInput{
					ID:      fmt.Sprintf("%d", addr.ID),
					Street:  addr.Street,
					City:    addr.City,
					State:   addr.State,
					ZipCode: addr.ZipCode,
				}
			}

			// Batch geocode via PostGIS.
			results, batchErr := gcClient.BatchGeocode(ctx, inputs)
			if batchErr != nil {
				log.Warn("geocode batch failed", zap.Error(batchErr))
				failed += len(inputs)
				continue
			}

			// Update addresses and associate with MSAs.
			for j, result := range results {
				if !result.Matched {
					failed++
					continue
				}

				addr := batch[j]
				if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, result.Latitude, result.Longitude, result.Source, result.Quality, result.CountyFIPS); updateErr != nil {
					log.Warn("failed to update address geocode",
						zap.Int64("address_id", addr.ID),
						zap.Error(updateErr),
					)
					failed++
					continue
				}
				geocoded++

				// Associate with MSAs.
				if assoc != nil {
					topN := cfg.Geo.TopMSAs
					if topN <= 0 {
						topN = 3
					}
					relations, assocErr := assoc.AssociateAddress(ctx, addr.ID, result.Latitude, result.Longitude, topN)
					if assocErr != nil {
						log.Warn("MSA association failed",
							zap.Int64("address_id", addr.ID),
							zap.Error(assocErr),
						)
					} else {
						msaCount += len(relations)
					}
				}
			}

			log.Info("batch progress",
				zap.Int("processed", end),
				zap.Int("total", len(addrs)),
				zap.Int("geocoded", geocoded),
			)
		}

		fmt.Printf("Backfill complete: %d geocoded, %d failed, %d MSA associations\n", geocoded, failed, msaCount)
		return nil
	},
}

func init() {
	geoBackfillCmd.Flags().Int("limit", 100, "maximum number of addresses to geocode")
	geoBackfillCmd.Flags().Int("batch-size", 1000, "batch size for geocoding")
	geoBackfillCmd.Flags().Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillCmd)
}
