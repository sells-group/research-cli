package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeo "github.com/sells-group/research-cli/internal/temporal/geo"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

var temporalWorkerCmd = &cobra.Command{
	Use:   "temporal-worker",
	Short: "Run a Temporal worker process",
	Long:  "Starts a Temporal worker that polls for and executes workflows/activities.",
}

var temporalFedsyncWorkerCmd = &cobra.Command{
	Use:   "fedsync",
	Short: "Run the fedsync Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := fedsync.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "temporal-worker fedsync: migrate")
		}

		// Create temp directory.
		tempDir := cfg.Fedsync.TempDir
		if err := os.MkdirAll(tempDir, 0o750); err != nil {
			return eris.Wrapf(err, "create temp dir %s", tempDir)
		}

		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			UserAgent:  cfg.Fedsync.EDGARUserAgent,
			MaxRetries: 3,
			Timeout:    30 * time.Minute,
		})

		syncLog := fedsync.NewSyncLog(pool)
		reg := dataset.NewRegistry(cfg)
		activities := temporalfedsync.NewActivities(pool, f, syncLog, reg, tempDir, cfg)

		w := worker.New(c, temporalpkg.FedsyncTaskQueue, worker.Options{})
		w.RegisterWorkflow(temporalfedsync.RunWorkflow)
		w.RegisterWorkflow(temporalfedsync.DatasetSyncWorkflow)
		w.RegisterActivity(activities)

		zap.L().Info("starting fedsync temporal worker",
			zap.String("task_queue", temporalpkg.FedsyncTaskQueue),
		)
		return w.Run(worker.InterruptCh())
	},
}

var temporalEnrichmentWorkerCmd = &cobra.Command{
	Use:   "enrichment",
	Short: "Run the enrichment Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		activities := temporalenrich.NewActivities(env.Pipeline)

		w := worker.New(c, temporalpkg.EnrichmentTaskQueue, worker.Options{})
		w.RegisterWorkflow(temporalenrich.EnrichCompanyWorkflow)
		w.RegisterWorkflow(temporalenrich.BatchEnrichWorkflow)
		w.RegisterActivity(activities)

		zap.L().Info("starting enrichment temporal worker",
			zap.String("task_queue", temporalpkg.EnrichmentTaskQueue),
		)
		return w.Run(worker.InterruptCh())
	},
}

var temporalGeoWorkerCmd = &cobra.Command{
	Use:   "geo",
	Short: "Run the geo backfill Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		gcClient := geocode.NewClient(pool,
			geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
			geocode.WithMaxRating(cfg.Geo.MaxRating),
			geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
		)
		cs := company.NewPostgresStore(pool)
		assoc := geo.NewAssociator(pool, cs)

		activities := temporalgeo.NewActivities(pool, cs, gcClient, assoc, cfg)

		w := worker.New(c, temporalpkg.GeoTaskQueue, worker.Options{})
		w.RegisterWorkflow(temporalgeo.BackfillWorkflow)
		w.RegisterActivity(activities)

		zap.L().Info("starting geo temporal worker",
			zap.String("task_queue", temporalpkg.GeoTaskQueue),
		)
		return w.Run(worker.InterruptCh())
	},
}

func init() {
	temporalWorkerCmd.AddCommand(temporalFedsyncWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalEnrichmentWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalGeoWorkerCmd)
	rootCmd.AddCommand(temporalWorkerCmd)
}
