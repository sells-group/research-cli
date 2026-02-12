package main

import (
	"strings"
	"sync/atomic"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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

var batchLimit int

var batchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Batch enrich companies from Notion queue",
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

		// Query queued leads from Notion
		leads, err := notion.QueryQueuedLeads(ctx, notionClient, cfg.Notion.LeadDB)
		if err != nil {
			return eris.Wrap(err, "query queued leads")
		}

		if len(leads) == 0 {
			zap.L().Info("no queued leads found")
			return nil
		}

		// Apply limit
		if batchLimit > 0 && len(leads) > batchLimit {
			leads = leads[:batchLimit]
		}

		zap.L().Info("processing batch",
			zap.Int("leads", len(leads)),
			zap.Int("concurrency", cfg.Batch.MaxConcurrentCompanies),
		)

		// Build pipeline
		p := pipeline.New(cfg, st, jinaClient, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, pppClient, questions, fields)

		// Process leads concurrently
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(cfg.Batch.MaxConcurrentCompanies)

		var succeeded, failed atomic.Int64

		for _, lead := range leads {
			company := leadToCompany(lead)
			g.Go(func() error {
				log := zap.L().With(zap.String("company", company.URL))

				result, err := p.Run(gctx, company)
				if err != nil {
					failed.Add(1)
					log.Error("enrichment failed", zap.Error(err))
					return nil // don't abort batch on individual failure
				}

				succeeded.Add(1)
				log.Info("enrichment complete",
					zap.Float64("score", result.Score),
					zap.Int("fields_found", len(result.Answers)),
				)
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return eris.Wrap(err, "batch processing")
		}

		zap.L().Info("batch complete",
			zap.Int64("succeeded", succeeded.Load()),
			zap.Int64("failed", failed.Load()),
		)
		return nil
	},
}

func init() {
	batchCmd.Flags().IntVar(&batchLimit, "limit", 100, "max number of leads to process")
	rootCmd.AddCommand(batchCmd)
}

func leadToCompany(page notionapi.Page) model.Company {
	c := model.Company{
		NotionPageID: string(page.ID),
	}

	if prop, ok := page.Properties["Name"]; ok {
		if tp, ok := prop.(*notionapi.TitleProperty); ok {
			for _, rt := range tp.Title {
				c.Name += rt.PlainText
			}
		}
	}

	if prop, ok := page.Properties["URL"]; ok {
		if up, ok := prop.(*notionapi.URLProperty); ok {
			c.URL = up.URL
		}
	}

	if prop, ok := page.Properties["SalesforceID"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			for _, rt := range rtp.RichText {
				c.SalesforceID += rt.PlainText
			}
		}
	}

	if prop, ok := page.Properties["Location"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			for _, rt := range rtp.RichText {
				c.Location += rt.PlainText
			}
		}
	}

	c.URL = strings.TrimSpace(c.URL)
	c.Name = strings.TrimSpace(c.Name)
	c.SalesforceID = strings.TrimSpace(c.SalesforceID)
	c.Location = strings.TrimSpace(c.Location)

	return c
}
