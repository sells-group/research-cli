package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply geo schema migrations",
	Long:  "Applies all pending SQL migrations to the geo schema in lexicographic order.",
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

		if err := geospatial.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo migrate")
		}

		zap.L().Info("all geo migrations applied successfully")
		return nil
	},
}

func init() {
	geoCmd.AddCommand(geoMigrateCmd)
}
