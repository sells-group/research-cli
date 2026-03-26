package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/docling"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/scraper"
	"github.com/sells-group/research-cli/internal/geospatial"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporaladv "github.com/sells-group/research-cli/internal/temporal/adv"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeo "github.com/sells-group/research-cli/internal/temporal/geo"
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
	temporaltigerload "github.com/sells-group/research-cli/internal/temporal/tigerload"
	"github.com/sells-group/research-cli/pkg/geocode"
)

var temporalWorkerCmd = &cobra.Command{
	Use:   "temporal-worker [queues...]",
	Short: "Run Temporal workers for specified queues (default: all)",
	Long: `Starts Temporal workers that poll for and execute workflows/activities.

Without arguments, starts workers for all queues. Specify queue names to run a subset:
  temporal-worker                         # all queues
  temporal-worker fedsync                 # fedsync only
  temporal-worker fedsync geo             # fedsync + geo
  temporal-worker enrichment adv-documents

Available queues: fedsync, geo, enrichment, adv-documents`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		// If no args and subcommand was used, fall through to subcommand handling.
		if len(args) == 0 && cmd.CalledAs() == "temporal-worker" {
			// Default: run all queues.
			args = temporalpkg.AllQueues()
		}

		if len(args) == 0 {
			return cmd.Help()
		}

		return runUnifiedWorker(cmd, args)
	},
}

func runUnifiedWorker(cmd *cobra.Command, queues []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Validate queue names.
	valid := make(map[string]bool)
	for _, q := range temporalpkg.AllQueues() {
		valid[q] = true
	}
	for _, q := range queues {
		if !valid[q] {
			return fmt.Errorf("unknown queue %q (available: %s)", q, strings.Join(temporalpkg.AllQueues(), ", "))
		}
	}

	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	// Build setups for requested queues.
	queueSet := make(map[string]bool)
	for _, q := range queues {
		queueSet[q] = true
	}

	var workers []worker.Worker

	if queueSet[temporalpkg.FedsyncTaskQueue] {
		w, err := buildFedsyncWorker(ctx, c)
		if err != nil {
			return eris.Wrap(err, "build fedsync worker")
		}
		workers = append(workers, w)
	}

	if queueSet[temporalpkg.GeoTaskQueue] {
		w, err := buildGeoWorker(ctx, c)
		if err != nil {
			return eris.Wrap(err, "build geo worker")
		}
		workers = append(workers, w)
	}

	if queueSet[temporalpkg.EnrichmentTaskQueue] {
		w, err := buildEnrichmentWorker(ctx, c)
		if err != nil {
			return eris.Wrap(err, "build enrichment worker")
		}
		workers = append(workers, w)
	}

	if queueSet[temporalpkg.ADVDocumentQueue] {
		w, err := buildADVWorker(ctx, c)
		if err != nil {
			return eris.Wrap(err, "build adv worker")
		}
		workers = append(workers, w)
	}

	zap.L().Info("starting temporal workers",
		zap.Strings("queues", queues),
	)

	// Start all workers.
	for _, w := range workers {
		if err := w.Start(); err != nil {
			return eris.Wrap(err, "start worker")
		}
	}

	// Block until interrupt.
	<-worker.InterruptCh()

	// Stop all workers gracefully.
	for _, w := range workers {
		w.Stop()
	}

	return nil
}

func buildFedsyncWorker(ctx context.Context, c client.Client) (worker.Worker, error) {
	if err := cfg.Validate("fedsync"); err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx); err != nil {
		return nil, eris.Wrap(err, "ensure schema")
	}

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return nil, err
	}

	tempDir := cfg.Fedsync.TempDir
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		return nil, eris.Wrapf(err, "create temp dir %s", tempDir)
	}

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
		UserAgent:  cfg.Fedsync.EDGARUserAgent,
		MaxRetries: 3,
		Timeout:    30 * time.Minute,
	})

	syncLog := fedsync.NewSyncLog(pool)
	closeSyncCache, err := attachSyncLogCache(ctx, syncLog)
	if err != nil {
		return nil, err
	}
	_ = closeSyncCache
	reg := dataset.NewRegistry(cfg)
	activities := temporalfedsync.NewActivities(pool, f, syncLog, reg, tempDir, cfg)

	w := worker.New(c, temporalpkg.FedsyncTaskQueue, worker.Options{})
	w.RegisterWorkflow(temporalfedsync.RunWorkflow)
	w.RegisterWorkflow(temporalfedsync.DatasetSyncWorkflow)
	w.RegisterActivity(activities)

	return w, nil
}

func buildGeoWorker(ctx context.Context, c client.Client) (worker.Worker, error) {
	if err := cfg.Validate("fedsync"); err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx); err != nil {
		return nil, eris.Wrap(err, "ensure schema")
	}

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return nil, err
	}

	gcClient := geocode.NewClient(pool,
		geocode.WithCacheEnabled(cfg.Geo.CacheEnabled),
		geocode.WithMaxRating(cfg.Geo.MaxRating),
		geocode.WithCacheTTLDays(cfg.Geo.CacheTTLDays),
	)
	cs := company.NewPostgresStore(pool)
	assoc := geo.NewAssociator(pool, cs)

	activities := temporalgeo.NewActivities(pool, cs, gcClient, assoc, cfg)

	// Tiger load dependencies.
	tigerTempDir := cfg.Tiger.TempDir
	if tigerTempDir == "" {
		tigerTempDir = "/tmp/tiger"
	}
	if err := os.MkdirAll(tigerTempDir, 0o750); err != nil {
		return nil, eris.Wrapf(err, "create tiger temp dir %s", tigerTempDir)
	}
	tigerActivities := temporaltigerload.NewActivities(pool, tigerTempDir, cfg)

	// Geoscraper dependencies.
	tempDir := cfg.Fedsync.TempDir
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		return nil, eris.Wrapf(err, "create temp dir %s", tempDir)
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
		MaxRetries: 3,
		Timeout:    30 * time.Minute,
	})
	syncLog := fedsync.NewSyncLog(pool)
	closeSyncCache, err := attachSyncLogCache(ctx, syncLog)
	if err != nil {
		return nil, err
	}
	_ = closeSyncCache
	scraperReg := geoscraper.NewRegistry()
	scraper.RegisterAll(scraperReg, cfg)
	queue := geospatial.NewGeocodeQueue(pool, nil, cfg.Geo.BatchSize)
	geoScraperActivities := temporalgeoscraper.NewActivities(pool, f, syncLog, scraperReg, queue, tempDir, cfg)

	w := worker.New(c, temporalpkg.GeoTaskQueue, worker.Options{})
	w.RegisterWorkflow(temporalgeo.BackfillWorkflow)
	w.RegisterActivity(activities)
	w.RegisterWorkflow(temporaltigerload.Workflow)
	w.RegisterWorkflow(temporaltigerload.TigerStateWorkflow)
	w.RegisterActivity(tigerActivities)
	w.RegisterWorkflow(temporalgeoscraper.ScrapeWorkflow)
	w.RegisterWorkflow(temporalgeoscraper.ScrapeSingleWorkflow)
	w.RegisterActivity(geoScraperActivities)

	return w, nil
}

func buildEnrichmentWorker(ctx context.Context, c client.Client) (worker.Worker, error) {
	env, err := initPipeline(ctx)
	if err != nil {
		return nil, err
	}
	// Note: env.Close() should be called when the process exits.
	// In the unified worker, this is handled by process shutdown.

	activities := temporalenrich.NewActivities(env.Pipeline)

	w := worker.New(c, temporalpkg.EnrichmentTaskQueue, worker.Options{})
	w.RegisterWorkflow(temporalenrich.EnrichCompanyWorkflow)
	w.RegisterWorkflow(temporalenrich.BatchEnrichWorkflow)
	w.RegisterActivity(activities)

	return w, nil
}

func buildADVWorker(ctx context.Context, c client.Client) (worker.Worker, error) {
	if err := ensureSchema(ctx); err != nil {
		return nil, eris.Wrap(err, "ensure schema")
	}

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return nil, err
	}

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
		UserAgent:  cfg.Fedsync.EDGARUserAgent,
		MaxRetries: 3,
		Timeout:    30 * time.Minute,
	})
	dc := docling.NewClient(cfg.Fedsync.DoclingURL, cfg.Fedsync.DoclingAPIKey)

	activities := &temporaladv.Activities{
		Pool:    pool,
		Fetcher: f,
		Docling: dc,
	}

	w := worker.New(c, temporalpkg.ADVDocumentQueue, worker.Options{})
	w.RegisterWorkflow(temporaladv.DocumentSyncWorkflow)
	w.RegisterActivity(activities)

	return w, nil
}

// Subcommands for backward compatibility.
var temporalFedsyncWorkerCmd = &cobra.Command{
	Use:   "fedsync",
	Short: "Run the fedsync Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runUnifiedWorker(cmd, []string{temporalpkg.FedsyncTaskQueue})
	},
}

var temporalEnrichmentWorkerCmd = &cobra.Command{
	Use:   "enrichment",
	Short: "Run the enrichment Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runUnifiedWorker(cmd, []string{temporalpkg.EnrichmentTaskQueue})
	},
}

var temporalGeoWorkerCmd = &cobra.Command{
	Use:   "geo",
	Short: "Run the geo Temporal worker (backfill + tigerload + geoscraper)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runUnifiedWorker(cmd, []string{temporalpkg.GeoTaskQueue})
	},
}

var temporalADVWorkerCmd = &cobra.Command{
	Use:   "adv",
	Short: "Run the ADV document Temporal worker",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runUnifiedWorker(cmd, []string{temporalpkg.ADVDocumentQueue})
	},
}

func init() {
	temporalWorkerCmd.AddCommand(temporalFedsyncWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalEnrichmentWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalGeoWorkerCmd)
	temporalWorkerCmd.AddCommand(temporalADVWorkerCmd)
	rootCmd.AddCommand(temporalWorkerCmd)
}
