package main

import (
	"context"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
)

var batchLimit int

var batchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Batch enrich companies from Notion queue",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		// Query queued leads from Notion
		leads, err := notion.QueryQueuedLeads(ctx, env.Notion, cfg.Notion.LeadDB)
		if err != nil {
			return eris.Wrap(err, "query queued leads")
		}

		return processBatch(ctx, leads, batchLimit, cfg.Batch.MaxConcurrentCompanies, func(ctx context.Context, company model.Company) (*model.EnrichmentResult, error) {
			return env.Pipeline.Run(ctx, company)
		})
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

// enrichFunc is the callback signature for running enrichment on a company.
type enrichFunc func(ctx context.Context, company model.Company) (*model.EnrichmentResult, error)

// processBatch applies limit, then processes leads concurrently using the given enrichment function.
func processBatch(ctx context.Context, leads []notionapi.Page, limit, concurrency int, enrich enrichFunc) error {
	if len(leads) == 0 {
		zap.L().Info("no queued leads found")
		return nil
	}

	// Apply limit
	if limit > 0 && len(leads) > limit {
		leads = leads[:limit]
	}

	zap.L().Info("processing batch",
		zap.Int("leads", len(leads)),
		zap.Int("concurrency", concurrency),
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var succeeded, failed atomic.Int64

	for _, lead := range leads {
		company := leadToCompany(lead)
		g.Go(func() error {
			log := zap.L().With(zap.String("company", company.URL))

			result, err := enrich(gctx, company)
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
}
