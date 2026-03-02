package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/analysis"
	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoAnalyzeMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply analysis schema migrations",
	Long:  "Applies all pending analysis SQL migrations to the geo schema in lexicographic order.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure base geo migrations are current first.
		if err := geospatial.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo analyze migrate: geo migrate")
		}

		if err := analysis.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo analyze migrate")
		}

		zap.L().Info("all analysis migrations applied successfully")
		return nil
	},
}

func init() {
	geoAnalyzeCmd.AddCommand(geoAnalyzeMigrateCmd)
}
