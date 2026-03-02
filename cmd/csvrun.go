package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/registry"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
)

var (
	csvrunCSV           string
	csvrunLimit         int
	csvrunConcurrency   int
	csvrunDryRun        bool
	csvrunOffline       bool
	csvrunOutput        string
	csvrunFormat        string
	csvrunCompare       bool
	csvrunCompareOutput string
)

var csvrunCmd = &cobra.Command{
	Use:   "csvrun",
	Short: "Run enrichment pipeline on a CSV file (Grata or SF Report)",
	Long: `Reads a CSV file directly into the enrichment pipeline.

Auto-detects CSV format:
  - Grata export: requires "Domain" column
  - SF Report: requires "Account Name" + "Account ID" columns

Supports two modes:
  - Real API mode (default): uses real Anthropic/Jina/Firecrawl APIs
  - Offline mode (--offline): uses stub clients for fully offline testing

Examples:
  # Dry run — parse CSV only, no pipeline
  research-cli csvrun --csv companies.csv --dry-run

  # Offline full pipeline (no API keys needed)
  research-cli csvrun --csv companies.csv --offline --limit 1

  # Real APIs, single company
  research-cli csvrun --csv companies.csv --limit 1 --output results.json

  # SF Report enrichment
  research-cli csvrun --csv report.csv --limit 1 --output enriched.csv`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		// Detect CSV format.
		csvFormat, err := pipeline.DetectCSVFormat(csvrunCSV)
		if err != nil {
			return eris.Wrap(err, "csvrun: detect csv format")
		}
		zap.L().Info("csvrun: detected format", zap.String("format", string(csvFormat)))

		// Parse CSV based on detected format.
		var companies []model.Company
		var sfReportCompanies []pipeline.SFReportCompany

		switch csvFormat {
		case pipeline.CSVFormatSFReport:
			sfReportCompanies, err = pipeline.ParseSFReportCSV(csvrunCSV)
			if err != nil {
				return eris.Wrap(err, "csvrun: parse sf-report csv")
			}
			companies = pipeline.CompaniesFromSFReport(sfReportCompanies)
			// Auto-set output format for SF reports.
			if csvrunFormat == "json" {
				csvrunFormat = "sf-report-csv"
			}
		case pipeline.CSVFormatGrata:
			companies, err = pipeline.ParseGrataCSV(csvrunCSV)
			if err != nil {
				return eris.Wrap(err, "csvrun: parse grata csv")
			}
		default:
			return eris.Errorf("csvrun: unrecognized csv format (expected Grata or SF Report headers)")
		}
		zap.L().Info("parsed csv", zap.Int("companies", len(companies)))

		// Apply limit.
		if csvrunLimit > 0 && csvrunLimit < len(companies) {
			companies = companies[:csvrunLimit]
			if sfReportCompanies != nil && csvrunLimit < len(sfReportCompanies) {
				sfReportCompanies = sfReportCompanies[:csvrunLimit]
			}
		}

		// Dry run: print parsed companies and exit.
		if csvrunDryRun {
			return printCompaniesJSON(companies)
		}

		// Validate API keys in real mode.
		if !csvrunOffline {
			if err := validateAPIKeys(); err != nil {
				return err
			}
		}

		// Initialize pipeline.
		var env *pipelineEnv
		if csvrunOffline {
			env, err = initOfflinePipeline(ctx)
		} else {
			env, err = initPipeline(ctx)
		}
		if err != nil {
			return eris.Wrap(err, "csvrun: init pipeline")
		}
		defer env.Close()

		// Process companies concurrently.
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(csvrunConcurrency)

		var mu sync.Mutex
		var results []*model.EnrichmentResult
		var succeeded, failed atomic.Int64

		for i, company := range companies {
			g.Go(func() error {
				logCompanyBanner(i, len(companies), company)

				result, runErr := env.Pipeline.Run(gCtx, company)
				if runErr != nil {
					failed.Add(1)
					zap.L().Error("csvrun: company failed",
						zap.String("company", company.Name),
						zap.Error(runErr),
					)
					return nil // don't abort batch on individual failure
				}

				succeeded.Add(1)
				logCompanyResult(result)
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
				return nil
			})
		}

		_ = g.Wait()

		// Batch summary.
		zap.L().Info("csvrun: batch complete",
			zap.Int("total", len(companies)),
			zap.Int64("succeeded", succeeded.Load()),
			zap.Int64("failed", failed.Load()),
		)

		// Write results.
		switch csvrunFormat {
		case "sf-report-csv":
			outPath := csvrunOutput
			if outPath == "" {
				outPath = "enrichment-sfreport.csv"
			}
			if err := pipeline.ExportSFReportCSV(results, sfReportCompanies, outPath); err != nil {
				return err
			}
			zap.L().Info("csvrun: sf-report csv written", zap.String("path", outPath))
		case "grata-csv":
			outPath := csvrunOutput
			if outPath == "" {
				outPath = "enrichment-grata.csv"
			}
			if err := pipeline.ExportGrataCSV(results, outPath); err != nil {
				return err
			}
		default:
			if err := writeResults(results); err != nil {
				return err
			}
		}

		// Comparison report.
		if csvrunCompare {
			grataFull, parseErr := pipeline.ParseGrataCSVFull(csvrunCSV)
			if parseErr != nil {
				zap.L().Error("csvrun: parse grata ground truth for comparison", zap.Error(parseErr))
			} else {
				comps := pipeline.CompareResults(grataFull, results)
				report := pipeline.FormatComparisonReport(comps)
				if csvrunCompareOutput != "" {
					if writeErr := os.WriteFile(csvrunCompareOutput, []byte(report), 0o644); writeErr != nil {
						zap.L().Error("csvrun: write comparison report", zap.Error(writeErr))
					} else {
						zap.L().Info("csvrun: comparison report written", zap.String("path", csvrunCompareOutput))
					}
				} else {
					fmt.Fprint(os.Stderr, report)
				}
			}
		}

		return nil
	},
}

func init() {
	csvrunCmd.Flags().StringVar(&csvrunCSV, "csv", "", "path to CSV file (required)")
	csvrunCmd.Flags().IntVar(&csvrunLimit, "limit", 0, "max companies to process (0 = all)")
	csvrunCmd.Flags().IntVar(&csvrunConcurrency, "concurrency", 3, "max companies to process concurrently")
	csvrunCmd.Flags().BoolVar(&csvrunDryRun, "dry-run", false, "parse CSV and print companies, skip pipeline")
	csvrunCmd.Flags().BoolVar(&csvrunOffline, "offline", false, "use stub clients (no API keys needed)")
	csvrunCmd.Flags().StringVar(&csvrunOutput, "output", "", "write results to file (default: stdout for JSON, auto-named for CSV)")
	csvrunCmd.Flags().StringVar(&csvrunFormat, "format", "json", "output format: json, grata-csv, or sf-report-csv (auto-detected for SF reports)")
	csvrunCmd.Flags().BoolVar(&csvrunCompare, "compare", false, "compare results against Grata ground truth from CSV")
	csvrunCmd.Flags().StringVar(&csvrunCompareOutput, "compare-output", "", "write comparison report to file (default: stderr)")
	_ = csvrunCmd.MarkFlagRequired("csv")
	rootCmd.AddCommand(csvrunCmd)
}

// initOfflinePipeline builds a pipeline with stub clients and fixture registries.
func initOfflinePipeline(ctx context.Context) (*pipelineEnv, error) {
	// Use SQLite store.
	dsn := cfg.Store.DatabaseURL
	if dsn == "" {
		dsn = "research.db"
	}
	st, err := store.NewSQLite(dsn)
	if err != nil {
		return nil, eris.Wrap(err, "csvrun: init sqlite store")
	}
	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return nil, eris.Wrap(err, "csvrun: migrate store")
	}

	// Load fixture registries.
	questions, err := registry.LoadQuestionsFromFile("testdata/questions.json")
	if err != nil {
		_ = st.Close()
		return nil, eris.Wrap(err, "csvrun: load question fixtures")
	}
	fields, err := registry.LoadFieldsFromFile("testdata/fields.json")
	if err != nil {
		_ = st.Close()
		return nil, eris.Wrap(err, "csvrun: load field fixtures")
	}

	// Stub clients.
	jinaClient := &pipeline.StubJinaClient{}
	firecrawlClient := &pipeline.StubFirecrawlClient{}
	anthropicClient := &pipeline.StubAnthropicClient{}
	perplexityClient := &pipeline.StubPerplexityClient{}
	sfClient := &pipeline.StubSalesforceClient{}
	notionClient := &pipeline.StubNotionClient{}

	// Build scrape chain: Local → Jina.
	matcher := scrape.NewPathMatcher(cfg.Crawl.ExcludePaths)
	chain := scrape.NewChain(matcher,
		scrape.NewLocalScraper(),
		scrape.NewJinaAdapter(jinaClient),
	)

	p := pipeline.New(cfg, st, chain, jinaClient, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, nil, nil, nil, nil, questions, fields)

	return &pipelineEnv{
		Store:     st,
		Pipeline:  p,
		Questions: questions,
		Fields:    fields,
		Notion:    notionClient,
	}, nil
}

// printCompaniesJSON prints parsed companies as indented JSON.
func printCompaniesJSON(companies []model.Company) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(companies)
}

// logCompanyBanner prints a banner log for the current company.
func logCompanyBanner(idx, total int, company model.Company) {
	zap.L().Info(fmt.Sprintf("======== company %d/%d ========", idx+1, total),
		zap.String("name", company.Name),
		zap.String("url", company.URL),
		zap.String("location", company.Location),
	)
}

// logCompanyResult logs a per-phase summary and final metrics.
func logCompanyResult(result *model.EnrichmentResult) {
	log := zap.L().With(zap.String("company", result.Company.Name))

	for _, phase := range result.Phases {
		log.Info("phase",
			zap.String("name", phase.Name),
			zap.String("status", string(phase.Status)),
			zap.Int64("duration_ms", phase.Duration),
			zap.Int("tokens", phase.TokenUsage.InputTokens+phase.TokenUsage.OutputTokens),
			zap.Float64("cost", phase.TokenUsage.Cost),
		)
	}

	log.Info("enrichment complete",
		zap.Float64("score", result.Score),
		zap.Int("fields_found", len(result.FieldValues)),
		zap.Int("answers", len(result.Answers)),
		zap.Int("total_tokens", result.TotalTokens),
		zap.Float64("total_cost", result.TotalCost),
	)
}

// validateAPIKeys checks that required API keys are configured for real mode
// and warns about optional missing keys.
func validateAPIKeys() error {
	var missing []string

	// Required keys — pipeline cannot function without these.
	if cfg.Anthropic.Key == "" {
		missing = append(missing, "RESEARCH_ANTHROPIC_KEY (required: extraction)")
	}
	if cfg.Jina.Key == "" {
		missing = append(missing, "RESEARCH_JINA_KEY (required: primary scraper)")
	}

	if len(missing) > 0 {
		return eris.Errorf("csvrun: missing required API keys:\n  %s\n\nSet these env vars or use --offline for stub mode", strings.Join(missing, "\n  "))
	}

	// Optional keys — log warnings but don't fail.
	if cfg.Firecrawl.Key == "" {
		zap.L().Warn("RESEARCH_FIRECRAWL_KEY not set, Firecrawl fallback scraper disabled")
	}
	if cfg.Perplexity.Key == "" {
		zap.L().Warn("RESEARCH_PERPLEXITY_KEY not set, LinkedIn enrichment phase will fail")
	}

	return nil
}

// writeResults writes results to the output file or stdout.
func writeResults(results []*model.EnrichmentResult) error {
	var w *os.File
	if csvrunOutput != "" {
		f, err := os.Create(csvrunOutput)
		if err != nil {
			return eris.Wrap(err, "csvrun: create output file")
		}
		defer f.Close() //nolint:errcheck
		w = f
	} else {
		w = os.Stdout
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
