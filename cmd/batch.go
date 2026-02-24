// Package main implements the research-cli command-line tool for account enrichment and federal data sync.
package main

import (
	"context"
	"fmt"
	"math"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/resilience"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/pkg/notion"
)

var (
	batchLimit    int
	reEnrichDays  int
	reEnrichLimit int
)

var batchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Batch enrich companies from Notion queue",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		// Query queued leads from Notion with retry for transient errors.
		var leads []notionapi.Page
		const maxQueryRetries = 3
		for attempt := range maxQueryRetries {
			leads, err = notion.QueryQueuedLeads(ctx, env.Notion, cfg.Notion.LeadDB)
			if err == nil {
				break
			}
			if attempt < maxQueryRetries-1 {
				backoff := time.Duration(1<<attempt) * time.Second
				zap.L().Warn("notion query failed, retrying",
					zap.Int("attempt", attempt+1),
					zap.Duration("backoff", backoff),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return eris.Wrap(ctx.Err(), "query queued leads cancelled")
				case <-time.After(backoff):
				}
			}
		}
		if err != nil {
			return eris.Wrap(err, "query queued leads")
		}

		dlqMaxRetries := cfg.Retry.DLQMaxRetries
		if dlqMaxRetries <= 0 {
			dlqMaxRetries = 3
		}

		// Enable deferred SF writes: collect intents during enrichment,
		// flush in bulk after all companies are processed.
		var intentsMu sync.Mutex
		var intents []*pipeline.SFWriteIntent

		env.Pipeline.SetDeferredWrites(func(intent *pipeline.SFWriteIntent) {
			intentsMu.Lock()
			intents = append(intents, intent)
			intentsMu.Unlock()
		})

		batchErr := processBatch(ctx, leads, batchLimit, cfg.Batch.MaxConcurrentCompanies, env.Notion, env.Store, dlqMaxRetries, func(ctx context.Context, company model.Company) (*model.EnrichmentResult, error) {
			return env.Pipeline.Run(ctx, company)
		})
		if batchErr != nil {
			return batchErr
		}

		// Flush deferred SF writes in bulk.
		if len(intents) > 0 {
			zap.L().Info("flushing deferred SF writes",
				zap.Int("intents", len(intents)),
			)
			summary, flushErr := env.Pipeline.FlushDeferredWrites(ctx, intents)
			if flushErr != nil {
				return eris.Wrap(flushErr, "flush deferred SF writes")
			}
			if summary != nil && len(summary.Failures) > 0 {
				zap.L().Warn("batch: SF write failures detected",
					zap.Int("total_failures", len(summary.Failures)),
				)
			}
		}

		return nil
	},
}

var retryFailedCmd = &cobra.Command{
	Use:   "retry-failed",
	Short: "Retry companies from the dead letter queue",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		// Query retryable entries from DLQ.
		entries, err := env.Store.DequeueDLQ(ctx, resilience.DLQFilter{
			ErrorType: "transient",
			Limit:     batchLimit,
		})
		if err != nil {
			return eris.Wrap(err, "query dead letter queue")
		}

		if len(entries) == 0 {
			zap.L().Info("no retryable entries in dead letter queue")
			return nil
		}

		zap.L().Info("retrying from dead letter queue",
			zap.Int("entries", len(entries)),
		)

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(cfg.Batch.MaxConcurrentCompanies)

		var succeeded, failed atomic.Int64

		for _, entry := range entries {
			g.Go(func() error {
				log := zap.L().With(
					zap.String("company", entry.Company.URL),
					zap.String("dlq_id", entry.ID),
					zap.Int("retry", entry.RetryCount+1),
				)

				result, enrichErr := env.Pipeline.Run(gctx, entry.Company)
				if enrichErr != nil {
					failed.Add(1)
					log.Error("dlq retry failed", zap.Error(enrichErr))

					// Increment retry count; compute next retry with exponential backoff.
					nextRetry := time.Now().Add(dlqBackoff(entry.RetryCount + 1))
					if incErr := env.Store.IncrementDLQRetry(gctx, entry.ID, nextRetry, enrichErr.Error()); incErr != nil {
						log.Warn("failed to increment dlq retry", zap.Error(incErr))
					}
					return nil
				}

				succeeded.Add(1)
				log.Info("dlq retry succeeded",
					zap.Float64("score", result.Score),
				)

				// Remove from DLQ on success.
				if rmErr := env.Store.RemoveDLQ(gctx, entry.ID); rmErr != nil {
					log.Warn("failed to remove dlq entry", zap.Error(rmErr))
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return eris.Wrap(err, "dlq retry processing")
		}

		zap.L().Info("dlq retry complete",
			zap.Int64("succeeded", succeeded.Load()),
			zap.Int64("failed", failed.Load()),
		)
		return nil
	},
}

var reEnrichCmd = &cobra.Command{
	Use:   "re-enrich",
	Short: "Re-enrich companies with stale data",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		cutoff := time.Now().AddDate(0, 0, -reEnrichDays)
		stale, err := env.Store.ListStaleCompanies(ctx, store.StaleCompanyFilter{
			LastEnrichedBefore: cutoff,
			Limit:              reEnrichLimit,
		})
		if err != nil {
			return eris.Wrap(err, "list stale companies")
		}

		if len(stale) == 0 {
			zap.L().Info("no stale companies found",
				zap.Int("days_threshold", reEnrichDays),
			)
			return nil
		}

		zap.L().Info("re-enriching stale companies",
			zap.Int("companies", len(stale)),
			zap.Int("days_threshold", reEnrichDays),
		)

		// Enable deferred SF writes.
		var intentsMu sync.Mutex
		var intents []*pipeline.SFWriteIntent

		env.Pipeline.SetDeferredWrites(func(intent *pipeline.SFWriteIntent) {
			intentsMu.Lock()
			intents = append(intents, intent)
			intentsMu.Unlock()
		})
		env.Pipeline.SetForceReExtract(true)

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(cfg.Batch.MaxConcurrentCompanies)

		var succeeded, failed atomic.Int64

		for _, sc := range stale {
			g.Go(func() error {
				log := zap.L().With(
					zap.String("company", sc.Company.URL),
					zap.String("last_run", sc.LastRunID),
				)

				result, enrichErr := env.Pipeline.Run(gctx, sc.Company)
				if enrichErr != nil {
					failed.Add(1)
					log.Error("re-enrichment failed", zap.Error(enrichErr))
					return nil
				}

				succeeded.Add(1)
				log.Info("re-enrichment complete",
					zap.Float64("score", result.Score),
					zap.Float64("prev_score", sc.LastScore),
				)
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return eris.Wrap(err, "re-enrich processing")
		}

		// Flush deferred SF writes.
		if len(intents) > 0 {
			zap.L().Info("flushing deferred SF writes",
				zap.Int("intents", len(intents)),
			)
			summary, flushErr := env.Pipeline.FlushDeferredWrites(ctx, intents)
			if flushErr != nil {
				return eris.Wrap(flushErr, "flush deferred SF writes")
			}
			if summary != nil && len(summary.Failures) > 0 {
				zap.L().Warn("re-enrich: SF write failures detected",
					zap.Int("total_failures", len(summary.Failures)),
				)
			}
		}

		zap.L().Info("re-enrich complete",
			zap.Int64("succeeded", succeeded.Load()),
			zap.Int64("failed", failed.Load()),
		)
		return nil
	},
}

func init() {
	batchCmd.Flags().IntVar(&batchLimit, "limit", 100, "max number of leads to process")
	batchCmd.AddCommand(retryFailedCmd)
	retryFailedCmd.Flags().IntVar(&batchLimit, "limit", 50, "max number of DLQ entries to retry")
	batchCmd.AddCommand(reEnrichCmd)
	reEnrichCmd.Flags().IntVar(&reEnrichDays, "days", 90, "re-enrich companies older than this many days")
	reEnrichCmd.Flags().IntVar(&reEnrichLimit, "limit", 50, "max number of stale companies to re-enrich")
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
// If notionClient is non-nil, failed enrichments update the Notion page status to "Failed".
// Failed companies with transient errors are enqueued to the dead letter queue for later retry.
func processBatch(ctx context.Context, leads []notionapi.Page, limit, concurrency int, notionClient notion.Client, st interface {
	EnqueueDLQ(ctx context.Context, entry resilience.DLQEntry) error
}, dlqMaxRetries int, enrich enrichFunc) error {
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

	var succeeded, failed, enqueued atomic.Int64

	for _, lead := range leads {
		company := leadToCompany(lead)
		g.Go(func() error {
			log := zap.L().With(zap.String("company", company.URL))

			result, err := enrich(gctx, company)
			if err != nil {
				failed.Add(1)
				log.Error("enrichment failed", zap.Error(err))
				if notionClient != nil && company.NotionPageID != "" {
					// Use a detached context so the Notion update succeeds even
					// if the batch context has been cancelled.
					nCtx, nCancel := context.WithTimeout(context.Background(), 10*time.Second)
					if nErr := updateNotionFailed(nCtx, notionClient, company.NotionPageID, err); nErr != nil {
						log.Warn("failed to update notion status to Failed", zap.Error(nErr))
					}
					nCancel()
				}

				// Enqueue transient failures to DLQ for later retry.
				if resilience.IsTransient(err) && st != nil {
					entry := resilience.DLQEntry{
						Company:      company,
						Error:        err.Error(),
						ErrorType:    "transient",
						RetryCount:   0,
						MaxRetries:   dlqMaxRetries,
						NextRetryAt:  time.Now().Add(dlqBackoff(0)),
						CreatedAt:    time.Now(),
						LastFailedAt: time.Now(),
					}
					dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 5*time.Second)
					if dlqErr := st.EnqueueDLQ(dlqCtx, entry); dlqErr != nil {
						log.Warn("failed to enqueue to DLQ", zap.Error(dlqErr))
					} else {
						enqueued.Add(1)
						log.Info("enqueued to dead letter queue for retry")
					}
					dlqCancel()
				}

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
		zap.Int64("enqueued_dlq", enqueued.Load()),
	)
	return nil
}

// dlqBackoff computes the next retry delay using exponential backoff.
// retry 0 → 1m, retry 1 → 5m, retry 2 → 25m, capped at 2h.
func dlqBackoff(retryCount int) time.Duration {
	base := 1 * time.Minute
	d := time.Duration(float64(base) * math.Pow(5, float64(retryCount)))
	if d > 2*time.Hour {
		d = 2 * time.Hour
	}
	return d
}

// updateNotionFailed sets the Notion page status to "Failed" when enrichment errors out.
func updateNotionFailed(ctx context.Context, client notion.Client, pageID string, _ error) error {
	now := notionapi.Date(time.Now())
	_, err := client.UpdatePage(ctx, pageID, &notionapi.PageUpdateRequest{
		Properties: notionapi.Properties{
			"Status": notionapi.StatusProperty{
				Status: notionapi.Status{
					Name: "Failed",
				},
			},
			"Last Enriched": notionapi.DateProperty{
				Date: &notionapi.DateObject{
					Start: &now,
				},
			},
		},
	})
	if err != nil {
		return eris.Wrap(err, fmt.Sprintf("batch: update notion page %s to Failed", pageID))
	}
	return nil
}
