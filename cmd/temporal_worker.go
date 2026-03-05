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
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/scraper"
	"github.com/sells-group/research-cli/internal/geospatial"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeo "github.com/sells-group/research-cli/internal/temporal/geo"
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
	temporaltigerload "github.com/sells-group/research-cli/internal/temporal/tigerload"

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

		var env *pipelineEnv
		if offline, _ := cmd.Flags().GetBool("offline"); offline {
			env, err = initOfflinePipeline(ctx)
		} else {
			env, err = initPipeline(ctx)
		}
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
	Short: "Run the geo Temporal worker (backfill + scrape)",
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

		// Backfill dependencies.
		gcClient := geocode.NewClient(pool,
			geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
			geocode.WithMaxRating(cfg.Geo.MaxRating),
			geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
		)
		cs := company.NewPostgresStore(pool)
		assoc := geo.NewAssociator(pool, cs)
		backfillActivities := temporalgeo.NewActivities(pool, cs, gcClient, assoc, cfg)

		// Scrape dependencies.
		tempDir := cfg.Fedsync.TempDir
		if err := os.MkdirAll(tempDir, 0o750); err != nil {
			return eris.Wrapf(err, "create temp dir %s", tempDir)
		}
		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			MaxRetries: 3,
			Timeout:    30 * time.Minute,
		})
		syncLog := fedsync.NewSyncLog(pool)
		scrapeReg := geoscraper.NewRegistry()
		scraper.RegisterAll(scrapeReg, cfg)
		queue := geospatial.NewGeocodeQueue(pool, nil, cfg.Geo.BatchSize)
		scrapeActivities := temporalgeoscraper.NewActivities(pool, f, syncLog, scrapeReg, queue, tempDir, cfg)

		// Tiger load dependencies.
		tigerTempDir := cfg.Tiger.TempDir
		if tigerTempDir == "" {
			tigerTempDir = "/tmp/tiger"
		}
		if err := os.MkdirAll(tigerTempDir, 0o750); err != nil {
			return eris.Wrapf(err, "create tiger temp dir %s", tigerTempDir)
		}
		tigerActivities := temporaltigerload.NewActivities(pool, tigerTempDir, cfg)

		w := worker.New(c, temporalpkg.GeoTaskQueue, worker.Options{})
		// Backfill workflows/activities.
		w.RegisterWorkflow(temporalgeo.BackfillWorkflow)
		w.RegisterActivity(backfillActivities)
		// Scrape workflows/activities.
		w.RegisterWorkflow(temporalgeoscraper.ScrapeWorkflow)
		w.RegisterWorkflow(temporalgeoscraper.ScrapeSingleWorkflow)
		w.RegisterActivity(scrapeActivities)
		// Tiger load workflows/activities.
		w.RegisterWorkflow(temporaltigerload.Workflow)
		w.RegisterWorkflow(temporaltigerload.TigerStateWorkflow)
		w.RegisterActivity(tigerActivities)

		zap.L().Info("starting geo temporal worker",
			zap.String("task_queue", temporalpkg.GeoTaskQueue),
		)
		return w.Run(worker.InterruptCh())
	},
}

var temporalTigerWorkerCmd = &cobra.Command{
	Use:   "tiger",
	Short: "Run the TIGER loader Temporal worker",
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

		tempDir := cfg.Tiger.TempDir
		if tempDir == "" {
			tempDir = "/tmp/tiger"
		}
		if err := os.MkdirAll(tempDir, 0o750); err != nil {
			return eris.Wrapf(err, "create temp dir %s", tempDir)
		}

		activities := temporaltigerload.NewActivities(pool, tempDir, cfg)

		w := worker.New(c, temporalpkg.TigerTaskQueue, worker.Options{})
		w.RegisterWorkflow(temporaltigerload.Workflow)
		w.RegisterWorkflow(temporaltigerload.TigerStateWorkflow)
		w.RegisterActivity(activities)

		zap.L().Info("starting tiger temporal worker",
			zap.String("task_queue", temporalpkg.TigerTaskQueue),
		)
		return w.Run(worker.InterruptCh())
	},
}

func init() {
	temporalEnrichmentWorkerCmd.Flags().Bool("offline", false,
		"use stub clients for offline testing (no API keys needed)")
	temporalWorkerCmd.AddCommand(temporalFedsyncWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalEnrichmentWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalGeoWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalTigerWorkerCmd)
	rootCmd.AddCommand(temporalWorkerCmd)
}
