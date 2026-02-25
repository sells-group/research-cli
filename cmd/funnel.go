package main

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/scorer"
)

var funnelCmd = &cobra.Command{
	Use:   "funnel",
	Short: "Run multi-pass firm scoring funnel",
	Long: `Orchestrates the multi-pass scoring pipeline end-to-end.

Pass 0 (ADV Score, $0/firm):    Query structured ADV data + brochure keywords
Pass 1 (Website Score, ~$0.01): Crawl websites + keyword matching
Pass 2 (T1 Extract, ~$0.12):    Lean Haiku extraction (P0/P1 questions only)
Pass 3 (Deep Enrich, ~$0.35):   Full pipeline with T2 escalation

Each pass filters firms for the next, creating a cost-efficient funnel.

Examples:
  # Run Pass 0 only (free, fast)
  funnel --max-pass 0

  # Run Pass 0 + Pass 1
  funnel --max-pass 1

  # Estimate costs without running
  funnel --max-pass 2 --dry-run

  # Full funnel with custom filters
  funnel --max-pass 2 --min-aum 200000000 --states TX,FL --save`,
	RunE: runFunnel,
}

func init() {
	f := funnelCmd.Flags()
	f.Int("max-pass", 0, "highest pass to run (0=ADV only, 1=+Website, 2=+T1, 3=+Deep)")
	f.Int64("min-aum", 0, "minimum AUM filter (overrides config)")
	f.Int64("max-aum", 0, "maximum AUM filter (overrides config)")
	f.String("states", "", "comma-separated state codes")
	f.String("geo-keywords", "", "comma-separated geography keywords")
	f.String("industry-keywords", "", "comma-separated industry keywords")
	f.Float64("min-score", 0, "minimum score threshold")
	f.Int("limit", 0, "maximum firms per pass (0=use config default)")
	f.Bool("dry-run", false, "estimate costs without running")
	f.Bool("save", false, "save results to fed_data.firm_scores")

	rootCmd.AddCommand(funnelCmd)
}

func runFunnel(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cfg.Validate("fedsync"); err != nil {
		return err
	}

	log := zap.L().With(zap.String("command", "funnel"))

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := fedsync.Migrate(ctx, pool); err != nil {
		return eris.Wrap(err, "funnel: migrate")
	}

	// Parse flags.
	maxPass, _ := cmd.Flags().GetInt("max-pass")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	save, _ := cmd.Flags().GetBool("save")

	if maxPass < 0 || maxPass > 3 {
		return eris.Errorf("funnel: --max-pass must be 0-3 (got %d)", maxPass)
	}

	// Build scorer config with overrides.
	scorerCfg := applyScorerOverrides(cmd, cfg.Scorer)
	if err := scorer.ValidateConfig(scorerCfg); err != nil {
		return err
	}

	if dryRun {
		printCostEstimate(maxPass, scorerCfg)
		return nil
	}

	// ---- Pass 0: ADV Score ----
	log.Info("funnel: starting Pass 0 (ADV Score)")
	advScorer := scorer.NewADVScorer(pool, scorerCfg)
	pass0Results, err := advScorer.Score(ctx, &scorer.ScoreFilters{
		MinAUM:   scorerCfg.MinAUM,
		MaxAUM:   scorerCfg.MaxAUM,
		States:   scorerCfg.TargetStates,
		MinScore: scorerCfg.MinScore,
		Limit:    scorerCfg.MaxFirms,
	})
	if err != nil {
		return eris.Wrap(err, "funnel: Pass 0")
	}

	pass0Passed := scorerCountPassed(pass0Results)
	log.Info("funnel: Pass 0 complete",
		zap.Int("scored", len(pass0Results)),
		zap.Int("passed", pass0Passed),
	)
	fmt.Printf("Pass 0 (ADV):     %d scored, %d passed (cost: $0.00)\n", len(pass0Results), pass0Passed)

	if save && len(pass0Results) > 0 {
		if err := scorer.SaveScores(ctx, pool, pass0Results, 0); err != nil {
			return eris.Wrap(err, "funnel: save Pass 0")
		}
	}

	if maxPass == 0 {
		return nil
	}

	// ---- Pass 1: Website Score ----
	pass0Survivors := scorerFilterPassed(pass0Results)
	if len(pass0Survivors) == 0 {
		fmt.Println("No firms passed Pass 0. Funnel complete.")
		return nil
	}

	log.Info("funnel: starting Pass 1 (Website Score)",
		zap.Int("candidates", len(pass0Survivors)),
	)

	webScorer := scorer.NewWebsiteScorer(nil, nil, scorerCfg)
	var pass1Results []scorer.FirmScore
	for i, firm := range pass0Survivors {
		ws, err := webScorer.Score(ctx, &firm)
		if err != nil {
			log.Warn("funnel: website scoring failed",
				zap.Int("crd_number", firm.CRDNumber),
				zap.Error(err),
			)
			// Keep the firm with its Pass 0 score if website scoring fails.
			pass1Results = append(pass1Results, firm)
			continue
		}
		pass1Results = append(pass1Results, ws.FirmScore)
		if (i+1)%100 == 0 {
			log.Info("funnel: Pass 1 progress",
				zap.Int("completed", i+1),
				zap.Int("total", len(pass0Survivors)),
			)
		}
	}

	pass1Passed := scorerCountPassed(pass1Results)
	log.Info("funnel: Pass 1 complete",
		zap.Int("scored", len(pass1Results)),
		zap.Int("passed", pass1Passed),
	)
	fmt.Printf("Pass 1 (Website): %d scored, %d passed (est cost: $%.2f)\n",
		len(pass1Results), pass1Passed, float64(len(pass1Results))*0.005)

	if save && len(pass1Results) > 0 {
		if err := scorer.SaveScores(ctx, pool, pass1Results, 1); err != nil {
			return eris.Wrap(err, "funnel: save Pass 1")
		}
	}

	if maxPass == 1 {
		return nil
	}

	// ---- Pass 2 & 3: T1 Extract and Deep Enrichment ----
	if maxPass >= 2 {
		fmt.Printf("\nPass 2 (T1 Extract) and Pass 3 (Deep Enrichment) not yet implemented.\n")
		fmt.Printf("Run the enrichment pipeline in sourcing mode for Pass 2:\n")
		fmt.Printf("  go run ./cmd run --url <domain> --mode sourcing\n")
	}

	return nil
}

func printCostEstimate(maxPass int, _ config.ScorerConfig) {
	// Estimate firm counts at each pass based on typical funnel rates.
	estimatedFirms := 15000

	fmt.Println("--- Cost Estimate (Dry Run) ---")
	fmt.Printf("Pass 0 (ADV Score):     %6d firms × $0.00  = $%8.2f\n", estimatedFirms, 0.0)

	totalCost := 0.0
	if maxPass >= 1 {
		pass1 := int(float64(estimatedFirms) * 0.30)
		cost1 := float64(pass1) * 0.005
		totalCost += cost1
		fmt.Printf("Pass 1 (Website Score): %6d firms × $0.005 = $%8.2f\n", pass1, cost1)

		if maxPass >= 2 {
			pass2 := int(float64(pass1) * 0.60)
			cost2 := float64(pass2) * 0.12
			totalCost += cost2
			fmt.Printf("Pass 2 (T1 Extract):   %6d firms × $0.12  = $%8.2f\n", pass2, cost2)

			if maxPass >= 3 {
				pass3 := int(float64(pass2) * 0.15)
				cost3 := float64(pass3) * 0.35
				totalCost += cost3
				fmt.Printf("Pass 3 (Deep Enrich):  %6d firms × $0.35  = $%8.2f\n", pass3, cost3)
			}
		}
	}
	fmt.Printf("\nEstimated total cost: $%.2f\n", totalCost)
}

func scorerCountPassed(results []scorer.FirmScore) int {
	n := 0
	for _, r := range results {
		if r.Passed {
			n++
		}
	}
	return n
}

func scorerFilterPassed(results []scorer.FirmScore) []scorer.FirmScore {
	var passed []scorer.FirmScore
	for _, r := range results {
		if r.Passed {
			passed = append(passed, r)
		}
	}
	return passed
}
