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
	Long:  "Geocodes existing ungeocoded addresses and optionally associates them with MSAs.",
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

		// Build geocode client.
		opts := []geocode.Option{
			geocode.WithRateLimit(50),
		}
		if cfg.Geo.FallbackGoogle && cfg.Google.Key != "" {
			opts = append(opts, geocode.WithGoogleAPIKey(cfg.Google.Key))
		}
		gcClient := geocode.NewClient(opts...)

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

			// Batch geocode.
			results, batchErr := gcClient.BatchGeocode(ctx, inputs)
			if batchErr != nil {
				log.Warn("geocode batch failed, falling back to individual",
					zap.Error(batchErr),
				)
				// Fall back to individual geocoding.
				results = make([]geocode.Result, len(inputs))
				for j, input := range inputs {
					r, gcErr := gcClient.Geocode(ctx, input)
					if gcErr != nil {
						failed++
						continue
					}
					results[j] = *r
				}
			}

			// Update addresses and associate with MSAs.
			for j, result := range results {
				if !result.Matched {
					failed++
					continue
				}

				addr := batch[j]
				if updateErr := cs.UpdateAddressGeocode(ctx, addr.ID, result.Latitude, result.Longitude, result.Source, result.Quality); updateErr != nil {
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
	geoBackfillCmd.Flags().Int("batch-size", 1000, "batch size for Census API")
	geoBackfillCmd.Flags().Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillCmd)
}
