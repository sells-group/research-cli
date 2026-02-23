package main

import (
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/advextract"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

var fedsyncExtractADVCmd = &cobra.Command{
	Use:   "extract-adv",
	Short: "Extract M&A intelligence from ADV filings",
	Long: `Extract structured M&A intelligence from SEC ADV filings using tiered Claude models.

Runs a 95-question extraction pipeline across ADV Part 1 (structured data),
Part 2 (brochure), and Part 3 (CRS) documents. Results are stored in
fed_data.adv_advisor_answers and fed_data.adv_fund_answers.

~17 questions are answered directly from Part 1 structured data at zero cost.
Remaining questions are extracted via Haiku (T1), Sonnet (T2), and Opus (T3).

Examples:
  # Single advisor
  fedsync extract-adv --crd 12345

  # Batch with filters
  fedsync extract-adv --limit 100 --filter-aum-min 500000000 --filter-state CA

  # T1 only (cheapest)
  fedsync extract-adv --limit 1000 --tier 1

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
	f.Int("tier", 3, "maximum tier to run (1=Haiku only, 2=+Sonnet, 3=+Opus)")
	f.Float64("max-cost", 0, "per-advisor cost cap in USD (0=unlimited)")
	f.String("filter-state", "", "filter advisors by state (e.g., CA, NY)")
	f.Int64("filter-aum-min", 0, "filter advisors by minimum AUM")
	f.Bool("dry-run", false, "estimate cost without running extraction")
	f.Bool("force", false, "re-extract even if already done")
	f.Bool("funds-only", false, "only extract fund-level questions")

	fedsyncCmd.AddCommand(fedsyncExtractADVCmd)
}

func runExtractADV(cmd *cobra.Command, args []string) error {
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

	// Run migrations.
	if err := fedsync.Migrate(ctx, pool); err != nil {
		return eris.Wrap(err, "fedsync extract-adv: migrate")
	}

	// Parse flags.
	crd, _ := cmd.Flags().GetInt("crd")
	limit, _ := cmd.Flags().GetInt("limit")
	maxTier, _ := cmd.Flags().GetInt("tier")
	maxCost, _ := cmd.Flags().GetFloat64("max-cost")
	filterState, _ := cmd.Flags().GetString("filter-state")
	filterAUMMin, _ := cmd.Flags().GetInt64("filter-aum-min")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	force, _ := cmd.Flags().GetBool("force")
	fundsOnly, _ := cmd.Flags().GetBool("funds-only")

	if maxTier < 1 || maxTier > 3 {
		return eris.Errorf("fedsync extract-adv: --tier must be 1, 2, or 3 (got %d)", maxTier)
	}

	// Create extractor.
	client := anthropic.NewClient(cfg.Anthropic.Key)
	extractor := advextract.NewExtractor(pool, client, advextract.ExtractorOpts{
		MaxTier:   maxTier,
		MaxCost:   maxCost,
		DryRun:    dryRun,
		FundsOnly: fundsOnly,
		Force:     force,
	})

	// Single advisor mode.
	if crd > 0 {
		log.Info("extracting single advisor",
			zap.Int("crd", crd),
			zap.Int("max_tier", maxTier),
			zap.Bool("dry_run", dryRun))

		if err := extractor.RunAdvisor(ctx, crd); err != nil {
			return eris.Wrapf(err, "fedsync extract-adv: CRD %d", crd)
		}

		fmt.Printf("Extraction complete for CRD %d\n", crd)
		return nil
	}

	// Batch mode: list advisors matching filters.
	store := advextract.NewStore(pool)
	crds, err := store.ListAdvisors(ctx, advextract.ListOpts{
		Limit:            limit,
		State:            strings.ToUpper(filterState),
		MinAUM:           filterAUMMin,
		IncludeExtracted: force,
	})
	if err != nil {
		return eris.Wrap(err, "fedsync extract-adv: list advisors")
	}

	if len(crds) == 0 {
		fmt.Println("No advisors found matching filters")
		return nil
	}

	log.Info("batch extraction starting",
		zap.Int("advisors", len(crds)),
		zap.Int("max_tier", maxTier),
		zap.Bool("dry_run", dryRun),
		zap.String("filter_state", filterState),
		zap.Int64("filter_aum_min", filterAUMMin),
	)

	if dryRun {
		fmt.Println(advextract.EstimateBatchCost(len(crds), maxTier))
		return nil
	}

	if err := extractor.RunBatch(ctx, crds); err != nil {
		return eris.Wrap(err, "fedsync extract-adv: batch")
	}

	fmt.Printf("Batch extraction complete: %d advisors processed\n", len(crds))
	return nil
}
