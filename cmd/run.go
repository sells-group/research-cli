package main

import (
	"encoding/json"
	"os"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/registry"
	anthropicpkg "github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
)

var (
	runURL  string
	runSFID string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run enrichment for a single company",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		// Init store
		st, err := initStore(ctx)
		if err != nil {
			return err
		}
		defer st.Close()

		if err := st.Migrate(ctx); err != nil {
			return eris.Wrap(err, "migrate store")
		}

		// Init clients
		notionClient := notion.NewClient(cfg.Notion.Token)
		anthropicClient := anthropicpkg.NewClient(cfg.Anthropic.Key)
		firecrawlClient := firecrawl.NewClient(cfg.Firecrawl.Key, firecrawl.WithBaseURL(cfg.Firecrawl.BaseURL))
		jinaClient := jina.NewClient(cfg.Jina.Key, jina.WithBaseURL(cfg.Jina.BaseURL))
		perplexityClient := perplexity.NewClient(cfg.Perplexity.Key, perplexity.WithBaseURL(cfg.Perplexity.BaseURL), perplexity.WithModel(cfg.Perplexity.Model))

		sfClient, err := initSalesforce()
		if err != nil {
			return err
		}

		// Init PPP client (optional)
		var pppClient ppp.Querier
		if cfg.PPP.URL != "" {
			pppClient, err = ppp.New(ctx, ppp.Config{
				URL:                 cfg.PPP.URL,
				SimilarityThreshold: cfg.PPP.SimilarityThreshold,
				MaxCandidates:       cfg.PPP.MaxCandidates,
			})
			if err != nil {
				zap.L().Warn("ppp client init failed, skipping PPP phase", zap.Error(err))
			} else {
				defer pppClient.Close()
			}
		}

		// Load registries
		questions, err := registry.LoadQuestionRegistry(ctx, notionClient, cfg.Notion.QuestionDB)
		if err != nil {
			return eris.Wrap(err, "load question registry")
		}
		fields, err := registry.LoadFieldRegistry(ctx, notionClient, cfg.Notion.FieldDB)
		if err != nil {
			return eris.Wrap(err, "load field registry")
		}

		zap.L().Info("registries loaded",
			zap.Int("questions", len(questions)),
			zap.Int("fields", len(fields.Fields)),
		)

		// Build pipeline
		p := pipeline.New(cfg, st, jinaClient, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, pppClient, questions, fields)

		company := model.Company{
			URL:          runURL,
			SalesforceID: runSFID,
		}

		result, err := p.Run(ctx, company)
		if err != nil {
			return eris.Wrap(err, "pipeline run")
		}

		zap.L().Info("enrichment complete",
			zap.String("company", company.URL),
			zap.Float64("score", result.Score),
			zap.Int("fields_found", len(result.Answers)),
			zap.Int("total_tokens", result.TotalTokens),
		)

		// Print result JSON to stdout
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	},
}

func init() {
	runCmd.Flags().StringVar(&runURL, "url", "", "company website URL (required)")
	runCmd.Flags().StringVar(&runSFID, "sf-id", "", "Salesforce account ID")
	_ = runCmd.MarkFlagRequired("url")
	rootCmd.AddCommand(runCmd)
}

