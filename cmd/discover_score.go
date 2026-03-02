package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/discovery"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

var discoverScoreCmd = &cobra.Command{
	Use:   "score",
	Short: "Score discovery candidates",
	Long:  "Apply T0 (programmatic), T1 (Haiku), or T2 (Sonnet) scoring to discovery candidates.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("discovery"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "discover.score"))

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		store := discovery.NewPostgresStore(pool)

		runID, _ := cmd.Flags().GetString("run-id")
		tier, _ := cmd.Flags().GetString("tier")
		limit, _ := cmd.Flags().GetInt("limit")

		if runID == "" {
			return eris.New("--run-id is required")
		}

		switch tier {
		case "t0":
			q, dq, err := discovery.RunT0(ctx, store, &cfg.Discovery, runID, limit)
			if err != nil {
				return eris.Wrap(err, "discover score t0")
			}
			log.Info("T0 complete", zap.Int("qualified", q), zap.Int("disqualified", dq))

		case "t1":
			if cfg.Anthropic.Key == "" {
				return eris.New("anthropic.key is required for T1 scoring")
			}
			ai := anthropic.NewClient(cfg.Anthropic.Key)
			scored, err := discovery.RunT1(ctx, store, ai, &cfg.Discovery, cfg.Anthropic.HaikuModel, runID, limit)
			if err != nil {
				return eris.Wrap(err, "discover score t1")
			}
			log.Info("T1 complete", zap.Int("scored", scored))

		case "t2":
			if cfg.Anthropic.Key == "" {
				return eris.New("anthropic.key is required for T2 scoring")
			}
			ai := anthropic.NewClient(cfg.Anthropic.Key)
			scored, err := discovery.RunT2(ctx, store, ai, &cfg.Discovery, cfg.Anthropic.SonnetModel, runID, limit)
			if err != nil {
				return eris.Wrap(err, "discover score t2")
			}
			log.Info("T2 complete", zap.Int("scored", scored))

		default:
			return eris.Errorf("unknown tier %q (use t0, t1, or t2)", tier)
		}

		return nil
	},
}

func init() {
	discoverScoreCmd.Flags().String("run-id", "", "discovery run ID (required)")
	discoverScoreCmd.Flags().String("tier", "t0", "scoring tier: t0, t1, t2")
	discoverScoreCmd.Flags().Int("limit", 1000, "maximum candidates to score")
	discoverCmd.AddCommand(discoverScoreCmd)
}
