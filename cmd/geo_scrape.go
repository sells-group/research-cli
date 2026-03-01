package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoScrapeCmd = &cobra.Command{
	Use:   "scrape",
	Short: "Run geo data scrapers",
	Long: `Run geo data scrapers to populate geo.* tables.

By default, runs all scrapers whose ShouldRun() returns true.
Use --category to restrict to a category, or --sources for specific scrapers.
Use --states to filter state-level scrapers by FIPS code.
Use --force to ignore ShouldRun() scheduling logic.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "geo.scrape"))

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure geo migrations are current.
		if err := geospatial.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo scrape: migrate")
		}

		// Parse flags.
		opts, err := parseScrapeOpts(cmd)
		if err != nil {
			return err
		}

		// Create temp directory with a unique run-specific subdirectory.
		tempDir := cfg.Fedsync.TempDir
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return eris.Wrapf(err, "geo scrape: create temp dir %s", tempDir)
		}
		runDir := filepath.Join(tempDir, fmt.Sprintf("geo-run-%d", time.Now().UnixNano()))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			return eris.Wrapf(err, "geo scrape: create run dir %s", runDir)
		}
		defer os.RemoveAll(runDir) //nolint:errcheck

		// Build fetcher.
		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			MaxRetries: 3,
			Timeout:    30 * time.Minute,
		})

		// Build engine dependencies.
		syncLog := fedsync.NewSyncLog(pool)
		reg := geoscraper.NewRegistry()
		queue := geospatial.NewGeocodeQueue(pool, nil, cfg.Geo.BatchSize)
		engine := geoscraper.NewEngine(pool, f, syncLog, reg, queue, runDir)

		log.Info("starting geo scrape",
			zap.Any("category", opts.Category),
			zap.Strings("sources", opts.Sources),
			zap.Strings("states", opts.States),
			zap.Bool("force", opts.Force),
		)

		if err := engine.Run(ctx, opts); err != nil {
			return eris.Wrap(err, "geo scrape")
		}

		zap.L().Info("geo scrape complete")
		return nil
	},
}

func init() {
	geoScrapeCmd.Flags().String("category", "", "restrict to category: national, state, on_demand")
	geoScrapeCmd.Flags().String("sources", "", "comma-separated scraper names (e.g., hifld,fema_flood)")
	geoScrapeCmd.Flags().String("states", "", "comma-separated state FIPS codes (e.g., 48,12,06)")
	geoScrapeCmd.Flags().Bool("force", false, "ignore ShouldRun() scheduling logic")
	geoCmd.AddCommand(geoScrapeCmd)
}

// parseScrapeOpts extracts geoscraper.RunOpts from the cobra command flags.
func parseScrapeOpts(cmd *cobra.Command) (geoscraper.RunOpts, error) {
	categoryStr, _ := cmd.Flags().GetString("category")
	sourcesStr, _ := cmd.Flags().GetString("sources")
	statesStr, _ := cmd.Flags().GetString("states")
	force, _ := cmd.Flags().GetBool("force")

	opts := geoscraper.RunOpts{
		Force: force,
	}

	if categoryStr != "" {
		c, err := geoscraper.ParseCategory(categoryStr)
		if err != nil {
			return geoscraper.RunOpts{}, err
		}
		opts.Category = &c
	}

	if sourcesStr != "" {
		opts.Sources = splitAndTrim(sourcesStr)
	}

	if statesStr != "" {
		opts.States = splitAndTrim(statesStr)
	}

	return opts, nil
}
