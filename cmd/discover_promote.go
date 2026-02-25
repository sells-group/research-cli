package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/discovery"
)

var discoverPromoteCmd = &cobra.Command{
	Use:   "promote",
	Short: "Promote qualified candidates to the enrichment pipeline",
	Long:  "Move discovery candidates above the score threshold into the companies table for enrichment.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("discovery"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "discover.promote"))

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		dStore := discovery.NewPostgresStore(pool)
		cStore := company.NewPostgresStore(pool)

		runID, _ := cmd.Flags().GetString("run-id")
		minScore, _ := cmd.Flags().GetFloat64("min-score")

		if runID == "" {
			return eris.New("--run-id is required")
		}

		result, err := discovery.Promote(ctx, dStore, cStore, runID, minScore)
		if err != nil {
			return eris.Wrap(err, "discover promote")
		}

		log.Info("promotion complete",
			zap.Int("promoted", result.Promoted),
			zap.Int("skipped", result.Skipped),
			zap.Int("errors", result.Errors),
		)

		return nil
	},
}

func init() {
	discoverPromoteCmd.Flags().String("run-id", "", "discovery run ID (required)")
	discoverPromoteCmd.Flags().Float64("min-score", 0.5, "minimum score for promotion")
	discoverCmd.AddCommand(discoverPromoteCmd)
}
