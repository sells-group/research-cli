package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/advextract"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

var fedsyncExtractADVCmd = &cobra.Command{
	Use:   "extract-adv",
	Short: "Extract M&A intelligence from ADV filings",
	Long: `Extract structured M&A intelligence from SEC ADV filings using tiered Claude models.

Runs a ~200-question extraction pipeline across ADV Part 1 (structured data),
Part 2 (brochure), and Part 3 (CRS) documents. Results are stored in
fed_data.adv_advisor_answers and fed_data.adv_fund_answers.

~29 questions are answered directly from Part 1 structured data (T0, $0).
~160 factual questions are extracted via Haiku (T1, ~$0.10/advisor).
~15 derived metrics are computed in Go from extracted facts ($0).
~8 optional judgment questions use Sonnet (T2, +$0.04/advisor).

Default tier is 1 (Haiku only, ~$0.10/advisor). Use --tier 2 for Sonnet synthesis.

Examples:
  # Single advisor (Haiku-only, default)
  fedsync extract-adv --crd 12345

  # Batch with filters
  fedsync extract-adv --limit 100 --filter-aum-min 500000000 --filter-state CA

  # With optional Sonnet synthesis questions
  fedsync extract-adv --crd 12345 --tier 2

  # Cost estimation
  fedsync extract-adv --limit 100 --dry-run

  # Force re-extract with cost cap
  fedsync extract-adv --crd 12345 --force --max-cost 5.00`,
	RunE: runExtractADV,
}

func init() {
	f := fedsyncExtractADVCmd.Flags()
	f.Int("crd", 0, "single advisor CRD number to extract")
	f.Int("limit", 0, "maximum number of advisors to process")
	f.Int("tier", 1, "maximum tier to run (1=Haiku only, 2=+Sonnet synthesis)")
	f.Float64("max-cost", 0, "per-advisor cost cap in USD (0=unlimited)")
	f.String("filter-state", "", "filter advisors by state (e.g., CA, NY)")
	f.Int64("filter-aum-min", 0, "filter advisors by minimum AUM")
	f.Bool("dry-run", false, "estimate cost without running extraction")
	f.Bool("force", false, "re-extract even if already done")
	f.Bool("funds-only", false, "only extract fund-level questions")

	fedsyncCmd.AddCommand(fedsyncExtractADVCmd)
}

func runExtractADV(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cfg.Validate("fedsync"); err != nil {
		return err
	}

	log := zap.L().With(zap.String("command", "fedsync.extract-adv"))

	// Validate anthropic key.
	if cfg.Anthropic.Key == "" {
		return eris.New("fedsync extract-adv: RESEARCH_ANTHROPIC_KEY is required")
	}

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Ensure schema is current via Atlas.
	if err := ensureSchema(ctx); err != nil {
		return eris.Wrap(err, "fedsync extract-adv: ensure schema")
	}

	// Parse flags.
	crd, _ := cmd.Flags().GetInt("crd")
	maxTier, _ := cmd.Flags().GetInt("tier")
	maxCost, _ := cmd.Flags().GetFloat64("max-cost")
	limit, _ := cmd.Flags().GetInt("limit")
	filterState, _ := cmd.Flags().GetString("filter-state")
	filterAUMMin, _ := cmd.Flags().GetInt64("filter-aum-min")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	force, _ := cmd.Flags().GetBool("force")
	fundsOnly, _ := cmd.Flags().GetBool("funds-only")

	client := anthropic.NewClient(cfg.Anthropic.Key)

	service := advextract.NewService(pool, client)
	result, err := service.Run(ctx, advextract.ServiceOptions{
		CRD:          crd,
		Limit:        limit,
		MaxTier:      maxTier,
		MaxCost:      maxCost,
		FilterState:  filterState,
		FilterAUMMin: filterAUMMin,
		DryRun:       dryRun,
		Force:        force,
		FundsOnly:    fundsOnly,
	})
	if err != nil {
		return eris.Wrap(err, "fedsync extract-adv")
	}

	if result.Mode == "single" {
		log.Info("single advisor extraction complete",
			zap.Int("crd", result.SingleCRD),
			zap.Int("max_tier", maxTier),
			zap.Bool("dry_run", dryRun),
		)
		printOutputf(cmd, "Extraction complete for CRD %d\n", result.SingleCRD)
		return nil
	}

	if result.AdvisorCount == 0 {
		printOutputln(cmd, "No advisors found matching filters")
		return nil
	}

	log.Info("batch extraction prepared",
		zap.Int("advisors", result.AdvisorCount),
		zap.Int("max_tier", maxTier),
		zap.Bool("dry_run", dryRun),
		zap.String("filter_state", filterState),
		zap.Int64("filter_aum_min", filterAUMMin),
	)

	if dryRun {
		printOutputln(cmd, result.CostEstimate)
		return nil
	}

	printOutputf(cmd, "Batch extraction complete: %d advisors processed\n", result.AdvisorCount)
	return nil
}
