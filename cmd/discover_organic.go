package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/discovery"
	"github.com/sells-group/research-cli/pkg/google"
)

var discoverOrganicCmd = &cobra.Command{
	Use:   "organic",
	Short: "Discover leads via Google Places MSA grid search",
	Long:  "Search for businesses within MSA grid cells using Google Places API.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("discovery"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "discover.organic"))

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		store := discovery.NewPostgresStore(pool)
		gClient := google.NewClient(cfg.Google.Key)

		msa, _ := cmd.Flags().GetString("msa")
		query, _ := cmd.Flags().GetString("query")
		cellKM, _ := cmd.Flags().GetFloat64("cell-km")
		limit, _ := cmd.Flags().GetInt("limit")

		if msa == "" {
			return eris.New("--msa (CBSA code) is required")
		}
		if query == "" {
			return eris.New("--query (text query) is required")
		}

		runCfg := discovery.RunConfig{
			Strategy: "organic",
			Params: map[string]any{
				"cbsa_code":  msa,
				"text_query": query,
				"max_cells":  float64(limit),
				"cell_km":    cellKM,
			},
		}

		strategy := discovery.NewOrganicStrategy(store, gClient, &cfg.Discovery)
		result, err := strategy.Run(ctx, runCfg)
		if err != nil {
			return eris.Wrap(err, "discover organic")
		}

		log.Info("organic discovery complete",
			zap.Int("found", result.CandidatesFound),
			zap.Float64("cost_usd", result.CostUSD),
		)

		return nil
	},
}

func init() {
	discoverOrganicCmd.Flags().String("msa", "", "CBSA code for target MSA (required)")
	discoverOrganicCmd.Flags().String("query", "", "text query for Places search (required)")
	discoverOrganicCmd.Flags().Float64("cell-km", 2.0, "grid cell size in kilometers")
	discoverOrganicCmd.Flags().Int("limit", 100, "maximum grid cells to search")
	discoverCmd.AddCommand(discoverOrganicCmd)
}
