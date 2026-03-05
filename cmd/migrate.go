package main

import (
	"fmt"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/migrate"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply sequential schema migrations",
	Long: `Apply sequential schema migrations via goose.

Runs all pending migrations against the database. Use --dry-run to preview
which migrations would be applied without making changes.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate: database URL is required (set store.database_url or fedsync.database_url)")
		}

		dryRun, _ := cmd.Flags().GetBool("dry-run")

		result, err := migrate.Apply(ctx, migrate.Options{
			URL:    dbURL,
			DryRun: dryRun,
		})
		if err != nil {
			return eris.Wrap(err, "migrate")
		}

		if dryRun {
			if result.Changes == "" {
				fmt.Println("No pending migrations.")
			} else {
				fmt.Println("Pending migrations:")
				fmt.Print(result.Changes)
			}
		} else {
			zap.L().Info("schema migration complete",
				zap.Int("migrations_applied", result.Applied),
			)
			if result.Applied == 0 {
				fmt.Println("Schema is up to date.")
			} else {
				fmt.Printf("Applied %d migration(s).\n", result.Applied)
			}
		}

		return nil
	},
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate status: database URL is required")
		}

		return migrate.Status(ctx, migrate.Options{URL: dbURL})
	},
}

func init() {
	migrateCmd.Flags().Bool("dry-run", false, "preview pending migrations without applying")
	migrateCmd.AddCommand(migrateStatusCmd)
	rootCmd.AddCommand(migrateCmd)
}
