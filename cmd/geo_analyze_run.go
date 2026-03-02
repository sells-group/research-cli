package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/analysis"
	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoAnalyzeRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run analysis pipeline",
	Long: `Run geospatial analysis pipeline.

By default, runs all analyzers in dependency order.
Use --category to restrict to a category, or --analyzers for specific names.
Use --force to skip validation checks.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "geo.analyze.run"))

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure geo + analysis migrations are current.
		if err := geospatial.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo analyze run: geo migrate")
		}
		if err := analysis.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "geo analyze run: analysis migrate")
		}

		opts, err := parseAnalyzeRunOpts(cmd)
		if err != nil {
			return err
		}

		// Build engine dependencies.
		alog := analysis.NewLog(pool)
		reg := analysis.NewRegistry()
		analysis.RegisterAll(reg)
		engine := analysis.NewEngine(pool, alog, reg)

		log.Info("starting analysis",
			zap.Any("category", opts.Category),
			zap.Strings("analyzers", opts.Analyzers),
			zap.Bool("force", opts.Force),
		)

		if err := engine.Run(ctx, opts); err != nil {
			return eris.Wrap(err, "geo analyze run")
		}

		zap.L().Info("analysis run complete")
		return nil
	},
}

func init() {
	geoAnalyzeRunCmd.Flags().String("category", "", "restrict to category: spatial, scoring, correlation, ranking, export")
	geoAnalyzeRunCmd.Flags().String("analyzers", "", "comma-separated analyzer names (e.g., proximity_matrix,parcel_scores)")
	geoAnalyzeRunCmd.Flags().Bool("force", false, "skip validation checks")
	geoAnalyzeCmd.AddCommand(geoAnalyzeRunCmd)
}

// parseAnalyzeRunOpts extracts analysis.RunOpts from the cobra command flags.
func parseAnalyzeRunOpts(cmd *cobra.Command) (analysis.RunOpts, error) {
	categoryStr, _ := cmd.Flags().GetString("category")
	analyzersStr, _ := cmd.Flags().GetString("analyzers")
	force, _ := cmd.Flags().GetBool("force")

	opts := analysis.RunOpts{
		Force: force,
	}

	if categoryStr != "" {
		c, err := analysis.ParseCategory(categoryStr)
		if err != nil {
			return analysis.RunOpts{}, err
		}
		opts.Category = &c
	}

	if analyzersStr != "" {
		opts.Analyzers = splitAndTrim(analyzersStr)
	}

	return opts, nil
}
