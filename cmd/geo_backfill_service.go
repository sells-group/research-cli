package main

import (
	"context"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	igeo "github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/internal/geobackfill"
	"github.com/sells-group/research-cli/pkg/geocode"
)

func runGeoEntityBackfill(
	ctx context.Context,
	cmd *cobra.Command,
	source geobackfill.Source,
	commandName string,
	emptyMessage string,
	summaryLabel string,
	allowTemporal bool,
	allowSkipGeocode bool,
) error {
	if allowTemporal {
		if useTemporal, _ := cmd.Flags().GetBool("temporal"); useTemporal {
			return runGeoBackfillViaTemporal(ctx, cmd, string(source))
		}
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
	skipGeocode := false
	if allowSkipGeocode {
		skipGeocode, _ = cmd.Flags().GetBool("skip-geocode")
	}
	skipBranches := false
	if flag := cmd.Flags().Lookup("skip-branches"); flag != nil {
		skipBranches, _ = cmd.Flags().GetBool("skip-branches")
	}

	store := company.NewPostgresStore(pool)
	var geocoder geocode.Client
	if !skipGeocode {
		geocoder = geocode.NewClient(pool,
			geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
			geocode.WithMaxRating(cfg.Geo.MaxRating),
			geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
			geocode.WithBatchConcurrency(concurrency),
		)
	}

	var assoc *igeo.Associator
	if !skipMSA {
		assoc = igeo.NewAssociator(pool, store)
	}

	svc := geobackfill.NewService(pool, store, geocoder, assoc, cfg)
	result, err := svc.Run(ctx, geobackfill.RunOptions{
		Source:       source,
		Limit:        limit,
		BatchSize:    batchSize,
		SkipMSA:      skipMSA,
		SkipBranches: skipBranches,
	})
	if err != nil {
		return eris.Wrapf(err, "%s: run backfill", commandName)
	}

	if result.TotalRecords == 0 {
		printOutputln(cmd, emptyMessage)
		return nil
	}

	zap.L().Info("geo entity backfill complete",
		zap.String("command", commandName),
		zap.String("source", string(source)),
		zap.Int("total_records", result.TotalRecords),
		zap.Int("created", result.Created),
		zap.Int("geocoded", result.Geocoded),
		zap.Int("linked", result.Linked),
		zap.Int("branches", result.Branches),
		zap.Int("msas", result.MSAs),
		zap.Int("failed", result.Failed),
	)

	printOutputf(cmd, "%s backfill complete: %d created, %d geocoded, %d linked, %d branches, %d MSA associations, %d failed\n",
		summaryLabel, result.Created, result.Geocoded, result.Linked, result.Branches, result.MSAs, result.Failed)

	return nil
}
