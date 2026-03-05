package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var geoAnalyzeMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply analysis schema migrations",
	Long:  "Applies declarative schema changes via Atlas to all managed schemas.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		if err := ensureSchema(ctx); err != nil {
			return eris.Wrap(err, "geo analyze migrate")
		}

		zap.L().Info("all analysis migrations applied successfully")
		return nil
	},
}

func init() {
	geoAnalyzeCmd.AddCommand(geoAnalyzeMigrateCmd)
}
