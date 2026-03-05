package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/migrate"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply schema migrations",
	Long: `Apply versioned schema migrations via Goose.

Runs all pending migrations against the database. On first run against
an existing database, automatically baselines version 1 (skipping the
baseline SQL) so subsequent migrations apply cleanly.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate: database URL is required (set store.database_url or fedsync.database_url)")
		}

		return migrate.Apply(ctx, dbURL)
	},
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current migration status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate status: database URL is required")
		}

		return migrate.Status(ctx, dbURL)
	},
}

var migrateBaselineCmd = &cobra.Command{
	Use:   "baseline",
	Short: "Record baseline version without running SQL",
	Long:  "Marks migration version 1 as applied without executing it. Use this for existing databases.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate baseline: database URL is required")
		}

		return migrate.Baseline(ctx, dbURL)
	},
}

func init() {
	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateBaselineCmd)
	rootCmd.AddCommand(migrateCmd)
}
