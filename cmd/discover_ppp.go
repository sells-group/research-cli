package main

import (
	"os/signal"
	"strings"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/discovery"
	"github.com/sells-group/research-cli/pkg/google"
)

var discoverPPPCmd = &cobra.Command{
	Use:   "ppp",
	Short: "Discover leads from PPP loan data",
	Long:  "Find PPP borrowers not yet in the companies table and look them up via Google Places.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("discovery"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "discover.ppp"))

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		store := discovery.NewPostgresStore(pool)
		gClient := google.NewClient(cfg.Google.Key)

		naicsStr, _ := cmd.Flags().GetString("naics")
		stateStr, _ := cmd.Flags().GetString("state")
		minApproval, _ := cmd.Flags().GetFloat64("min-approval")
		limit, _ := cmd.Flags().GetInt("limit")
		runScore, _ := cmd.Flags().GetBool("score")

		var naics, states []string
		if naicsStr != "" {
			naics = splitAndTrim(naicsStr)
		}
		if stateStr != "" {
			states = splitAndTrim(stateStr)
		}

		runCfg := discovery.RunConfig{
			Strategy: "ppp",
			Params: map[string]any{
				"naics":        naics,
				"states":       states,
				"min_approval": minApproval,
				"limit":        float64(limit),
			},
		}

		strategy := discovery.NewPPPStrategy(store, gClient, &cfg.Discovery)
		result, err := strategy.Run(ctx, runCfg)
		if err != nil {
			return eris.Wrap(err, "discover ppp")
		}

		log.Info("PPP discovery complete",
			zap.Int("found", result.CandidatesFound),
			zap.Float64("cost_usd", result.CostUSD),
		)

		if runScore && result.CandidatesFound > 0 {
			log.Info("running T0 disqualification")
			q, dq, err := discovery.RunT0(ctx, store, &cfg.Discovery, "", limit)
			if err != nil {
				return eris.Wrap(err, "discover ppp: t0 scoring")
			}
			log.Info("T0 complete", zap.Int("qualified", q), zap.Int("disqualified", dq))
		}

		return nil
	},
}

func init() {
	discoverPPPCmd.Flags().String("naics", "", "NAICS codes (comma-separated)")
	discoverPPPCmd.Flags().String("state", "", "US state codes (comma-separated)")
	discoverPPPCmd.Flags().Float64("min-approval", 150000, "minimum PPP approval amount")
	discoverPPPCmd.Flags().Int("limit", 1000, "maximum borrowers to process")
	discoverPPPCmd.Flags().Bool("score", false, "run T0 disqualification after discovery")
	discoverCmd.AddCommand(discoverPPPCmd)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
