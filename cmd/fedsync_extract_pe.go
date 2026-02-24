package main

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/peextract"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
)

var fedsyncExtractPECmd = &cobra.Command{
	Use:   "extract-pe",
	Short: "Extract intelligence from PE firm websites",
	Long: `Extract structured intelligence from PE firm websites using tiered Claude models.

Identifies PE firms from ADV owner data, crawls their websites, and extracts
27 questions about firm identity, leadership, portfolio, strategy, and financials.
Results are stored in fed_data.pe_answers and fed_data.mv_pe_intelligence.

25 factual questions are extracted via Haiku (T1, ~$0.04/firm).
2 optional synthesis questions use Sonnet (T2, +$0.02/firm).

Default tier is 1 (Haiku only). Use --tier 2 for Sonnet synthesis.

Examples:
  # Identify PE firms from ADV data
  fedsync extract-pe --identify-only

  # Single firm by ID
  fedsync extract-pe --firm-id 42

  # Single firm by name
  fedsync extract-pe --firm "Focus Financial Partners"

  # Batch extraction
  fedsync extract-pe --limit 50

  # Cost estimation
  fedsync extract-pe --limit 100 --dry-run

  # Force re-extract with Sonnet synthesis
  fedsync extract-pe --firm-id 42 --tier 2 --force`,
	RunE: runExtractPE,
}

func init() {
	f := fedsyncExtractPECmd.Flags()
	f.String("firm", "", "PE firm name to extract")
	f.Int64("firm-id", 0, "PE firm ID to extract")
	f.Int("limit", 0, "maximum number of firms to process")
	f.Int("tier", 1, "maximum tier to run (1=Haiku only, 2=+Sonnet synthesis)")
	f.Float64("max-cost", 0, "per-firm cost cap in USD (0=unlimited)")
	f.Int("min-rias", 2, "minimum RIA count filter for identification")
	f.Bool("identify-only", false, "only identify PE firms, don't extract")
	f.Bool("dry-run", false, "estimate cost without running extraction")
	f.Bool("force", false, "re-extract even if already done")

	fedsyncCmd.AddCommand(fedsyncExtractPECmd)
}

func runExtractPE(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cfg.Validate("fedsync"); err != nil {
		return err
	}

	log := zap.L().With(zap.String("command", "fedsync.extract-pe"))

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Run migrations.
	if err := fedsync.Migrate(ctx, pool); err != nil {
		return eris.Wrap(err, "fedsync extract-pe: migrate")
	}

	// Parse flags.
	firmName, _ := cmd.Flags().GetString("firm")
	firmID, _ := cmd.Flags().GetInt64("firm-id")
	limit, _ := cmd.Flags().GetInt("limit")
	maxTier, _ := cmd.Flags().GetInt("tier")
	maxCost, _ := cmd.Flags().GetFloat64("max-cost")
	minRIAs, _ := cmd.Flags().GetInt("min-rias")
	identifyOnly, _ := cmd.Flags().GetBool("identify-only")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	force, _ := cmd.Flags().GetBool("force")

	if maxTier < 1 || maxTier > 2 {
		return eris.Errorf("fedsync extract-pe: --tier must be 1 or 2 (got %d)", maxTier)
	}

	store := peextract.NewStore(pool)

	// Identify PE firms.
	candidates, err := peextract.IdentifyPEFirms(ctx, pool, minRIAs)
	if err != nil {
		return eris.Wrap(err, "fedsync extract-pe: identify firms")
	}

	// Batch-resolve websites for candidates (with override support).
	peextract.ResolveWebsites(ctx, pool, store, candidates)

	// Persist candidates.
	persisted, err := peextract.PersistCandidates(ctx, store, candidates)
	if err != nil {
		return eris.Wrap(err, "fedsync extract-pe: persist candidates")
	}

	log.Info("PE firms identified",
		zap.Int("candidates", len(candidates)),
		zap.Int("persisted", persisted))

	if identifyOnly {
		fmt.Printf("PE firms identified: %d\n", len(candidates))
		fmt.Printf("%-50s %-25s %5s %s\n", "FIRM NAME", "TYPE", "RIAs", "WEBSITE")
		fmt.Printf("%-50s %-25s %5s %s\n", "─────────", "────", "────", "───────")
		for _, c := range candidates {
			website := c.WebsiteURL
			if website == "" {
				website = "(none)"
			}
			fmt.Printf("%-50s %-25s %5d %s\n",
				truncatePE(c.OwnerName, 50),
				truncatePE(c.OwnerType, 25),
				c.RIACount,
				website)
		}
		return nil
	}

	// Validate anthropic key for extraction.
	if cfg.Anthropic.Key == "" {
		return eris.New("fedsync extract-pe: RESEARCH_ANTHROPIC_KEY is required for extraction")
	}

	// Create scrape chain: Local → Jina → Firecrawl.
	matcher := scrape.NewPathMatcher(nil)
	var scrapers []scrape.Scraper
	scrapers = append(scrapers, scrape.NewLocalScraper())

	if cfg.Jina.Key != "" {
		scrapers = append(scrapers, scrape.NewJinaAdapter(jina.NewClient(cfg.Jina.Key)))
	}
	if cfg.Firecrawl.Key != "" {
		scrapers = append(scrapers, scrape.NewFirecrawlAdapter(
			firecrawl.NewClient(cfg.Firecrawl.Key)))
	}

	chain := scrape.NewChain(matcher, scrapers...)
	if cfg.Firecrawl.Key != "" {
		chain = chain.WithFirecrawlClient(firecrawl.NewClient(cfg.Firecrawl.Key))
	}

	// Create extractor.
	client := anthropic.NewClient(cfg.Anthropic.Key)
	extractor := peextract.NewExtractor(pool, client, chain, peextract.ExtractorOpts{
		MaxTier: maxTier,
		MaxCost: maxCost,
		DryRun:  dryRun,
		Force:   force,
	})

	// Single firm mode.
	if firmID > 0 {
		log.Info("extracting single PE firm",
			zap.Int64("firm_id", firmID),
			zap.Int("max_tier", maxTier))

		if err := extractor.RunFirm(ctx, firmID); err != nil {
			return eris.Wrapf(err, "fedsync extract-pe: firm %d", firmID)
		}

		fmt.Printf("Extraction complete for PE firm %d\n", firmID)
		return nil
	}

	if firmName != "" {
		firm, err := store.LoadFirmByName(ctx, firmName)
		if err != nil {
			return eris.Wrapf(err, "fedsync extract-pe: lookup firm %q", firmName)
		}
		if firm == nil {
			return eris.Errorf("fedsync extract-pe: firm %q not found in pe_firms", firmName)
		}

		log.Info("extracting single PE firm",
			zap.Int64("firm_id", firm.PEFirmID),
			zap.String("firm_name", firm.FirmName),
			zap.Int("max_tier", maxTier))

		if err := extractor.RunFirm(ctx, firm.PEFirmID); err != nil {
			return eris.Wrapf(err, "fedsync extract-pe: firm %q", firmName)
		}

		fmt.Printf("Extraction complete for %s (firm %d)\n", firm.FirmName, firm.PEFirmID)
		return nil
	}

	// Batch mode.
	firmIDs, err := store.ListPEFirms(ctx, peextract.ListOpts{
		Limit:            limit,
		MinRIAs:          minRIAs,
		IncludeExtracted: force,
	})
	if err != nil {
		return eris.Wrap(err, "fedsync extract-pe: list firms")
	}

	if len(firmIDs) == 0 {
		fmt.Println("No PE firms found matching filters")
		return nil
	}

	log.Info("batch extraction starting",
		zap.Int("firms", len(firmIDs)),
		zap.Int("max_tier", maxTier),
		zap.Bool("dry_run", dryRun))

	if dryRun {
		fmt.Println(peextract.EstimateBatchCost(len(firmIDs), maxTier))
		return nil
	}

	if err := extractor.RunBatch(ctx, firmIDs); err != nil {
		return eris.Wrap(err, "fedsync extract-pe: batch")
	}

	fmt.Printf("Batch extraction complete: %d PE firms processed\n", len(firmIDs))
	return nil
}

// truncatePE truncates a string with ellipsis (package-level truncate exists in fedsync_status.go).
func truncatePE(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
