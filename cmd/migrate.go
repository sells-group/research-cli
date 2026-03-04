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
	Short: "Apply declarative schema changes",
	Long: `Apply declarative schema changes via Atlas.

Compares the desired schema (embedded SQL files) against the live database
and applies any necessary changes. Use --dry-run to preview without applying.`,
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
			URL:         dbURL,
			DevURL:      cfg.Atlas.DevURL,
			DryRun:      dryRun,
			AutoApprove: !dryRun,
			BinaryPath:  cfg.Atlas.BinaryPath,
		})
		if err != nil {
			return eris.Wrap(err, "migrate")
		}

		if dryRun {
			if result.Changes == "" {
				fmt.Println("No schema changes needed.")
			} else {
				fmt.Println("Planned changes:")
				fmt.Println(result.Changes)
			}
		} else {
			zap.L().Info("schema migration complete",
				zap.Int("changes_applied", result.Applied),
			)
			if result.Applied == 0 {
				fmt.Println("Schema is up to date.")
			} else {
				fmt.Printf("Applied %d schema changes.\n", result.Applied)
			}
		}

		return nil
	},
}

var migrateInspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Dump current database schema as HCL",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		dbURL := cfg.Fedsync.DatabaseURL
		if dbURL == "" {
			dbURL = cfg.Store.DatabaseURL
		}
		if dbURL == "" {
			return eris.New("migrate inspect: database URL is required")
		}

		result, err := migrate.Inspect(ctx, migrate.Options{
			URL:        dbURL,
			BinaryPath: cfg.Atlas.BinaryPath,
		})
		if err != nil {
			return eris.Wrap(err, "migrate inspect")
		}

		fmt.Println(result)
		return nil
	},
}

func init() {
	migrateCmd.Flags().Bool("dry-run", false, "preview changes without applying")
	migrateCmd.AddCommand(migrateInspectCmd)
	rootCmd.AddCommand(migrateCmd)
}
