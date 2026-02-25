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

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/tiger"
)

var tigerloadCmd = &cobra.Command{
	Use:   "tigerload",
	Short: "Load TIGER/Line shapefiles into PostGIS",
	Long: `Downloads Census TIGER/Line shapefiles and loads them into tiger_data.* tables
for use by the PostGIS tiger geocoder. Required before geocoding can work.

By default loads all required products for all 50 states + DC.
Use --states to restrict to specific states, --tables for specific products.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		log := zap.L().With(zap.String("command", "tigerload"))

		// Show status and exit if --status flag is set.
		showStatus, _ := cmd.Flags().GetBool("status")
		if showStatus {
			return printTigerStatus(ctx, pool)
		}

		// Ensure migrations are current (creates tiger_data schema + extension).
		if err := fedsync.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "tigerload: migrate")
		}

		// Parse flags.
		statesStr, _ := cmd.Flags().GetString("states")
		tablesStr, _ := cmd.Flags().GetString("tables")
		year, _ := cmd.Flags().GetInt("year")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		incremental, _ := cmd.Flags().GetBool("incremental")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		opts := tiger.LoadOptions{
			Year:        year,
			TempDir:     cfg.Tiger.TempDir,
			Concurrency: concurrency,
			Incremental: incremental,
			DryRun:      dryRun,
		}

		if statesStr != "" {
			opts.States = toUpper(splitAndTrim(statesStr))
		} else if len(cfg.Tiger.States) > 0 {
			opts.States = cfg.Tiger.States
		}

		if tablesStr != "" {
			opts.Tables = toUpper(splitAndTrim(tablesStr))
		} else if len(cfg.Tiger.Tables) > 0 {
			opts.Tables = cfg.Tiger.Tables
		}

		// Use config values as defaults.
		if opts.Year == 0 {
			opts.Year = cfg.Tiger.Year
		}
		if opts.Concurrency == 0 {
			opts.Concurrency = cfg.Tiger.Concurrency
		}

		log.Info("starting TIGER data load",
			zap.Int("year", opts.Year),
			zap.Strings("states", opts.States),
			zap.Strings("tables", opts.Tables),
			zap.Bool("incremental", opts.Incremental),
			zap.Bool("dry_run", opts.DryRun),
			zap.Int("concurrency", opts.Concurrency),
		)

		if err := tiger.Load(ctx, pool, opts); err != nil {
			return eris.Wrap(err, "tigerload")
		}

		fmt.Println("TIGER data load complete")
		return nil
	},
}

func init() {
	tigerloadCmd.Flags().String("states", "", "comma-separated state abbreviations (default: all 50 + DC)")
	tigerloadCmd.Flags().Int("year", 0, "TIGER/Line year (default: from config or 2024)")
	tigerloadCmd.Flags().String("tables", "", "comma-separated product names (default: all required)")
	tigerloadCmd.Flags().Bool("incremental", true, "skip already-loaded state/table/year combos")
	tigerloadCmd.Flags().Bool("dry-run", false, "download and validate without loading")
	tigerloadCmd.Flags().Int("concurrency", 0, "parallel state downloads (default: from config or 3)")
	tigerloadCmd.Flags().Bool("status", false, "show current TIGER data load status")
	rootCmd.AddCommand(tigerloadCmd)
}

// printTigerStatus displays the current TIGER data load status.
func printTigerStatus(ctx context.Context, pool *pgxpool.Pool) error {
	status, err := tiger.LoadStatus(ctx, pool)
	if err != nil {
		return eris.Wrap(err, "tigerload: get status")
	}

	if len(status) == 0 {
		fmt.Println("No TIGER data loaded yet")
		return nil
	}

	fmt.Printf("%-6s %-6s %-15s %-6s %10s %12s %s\n",
		"FIPS", "State", "Table", "Year", "Rows", "Duration", "Loaded At")
	fmt.Println(strings.Repeat("-", 80))

	for _, s := range status {
		fmt.Printf("%-6s %-6s %-15s %-6d %10d %10dms %s\n",
			s.StateFIPS, s.StateAbbr, s.TableName, s.Year,
			s.RowCount, s.DurationMs, s.LoadedAt.Format("2006-01-02 15:04"))
	}

	return nil
}

// toUpper uppercases all strings in a slice.
func toUpper(ss []string) []string {
	for i, s := range ss {
		ss[i] = strings.ToUpper(s)
	}
	return ss
}
