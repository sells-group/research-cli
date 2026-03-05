package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var geoMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply geo schema migrations",
	Long:  "Applies declarative schema changes via Atlas to all managed schemas.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		if err := ensureSchema(ctx); err != nil {
			return eris.Wrap(err, "geo migrate")
		}

		zap.L().Info("all geo migrations applied successfully")
		return nil
	},
}

func init() {
	geoCmd.AddCommand(geoMigrateCmd)
}
