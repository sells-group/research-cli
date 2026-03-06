// Package geoscraper provides Temporal workflows and activities for geo data scraping.
package geoscraper

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ScrapeParams is the input for ScrapeWorkflow.
type ScrapeParams struct {
	Category *string  `json:"category,omitempty"` // "national", "state", "on_demand"
	Sources  []string `json:"sources,omitempty"`
	States   []string `json:"states,omitempty"`
	Force    bool     `json:"force"`
}

// ScraperOutcome records the result of a single scraper sync.
type ScraperOutcome struct {
	Scraper    string `json:"scraper"`
	Status     string `json:"status"` // "complete", "failed", "skipped"
	RowsSynced int64  `json:"rows_synced,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ScrapeResult is the output of ScrapeWorkflow.
type ScrapeResult struct {
	Outcomes []ScraperOutcome `json:"outcomes"`
	Synced   int              `json:"synced"`
	Failed   int              `json:"failed"`
}

// ScrapeProgress is returned by the scrape_progress query.
type ScrapeProgress struct {
	Total     int              `json:"total"`
	Completed int              `json:"completed"`
	Failed    int              `json:"failed"`
	Running   int              `json:"running"`
	Outcomes  []ScraperOutcome `json:"outcomes"`
}

// ScrapeSingleParams is the input for ScrapeSingleWorkflow.
type ScrapeSingleParams struct {
	Scraper string `json:"scraper"`
}

// ScrapeSingleResult is the output of ScrapeSingleWorkflow.
type ScrapeSingleResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// ScrapeWorkflow orchestrates the sync of selected geo scrapers.
// It selects scrapers, then fans out ScrapeSingleWorkflow child workflows
// with a max concurrency of 5.
func ScrapeWorkflow(ctx workflow.Context, params ScrapeParams) (*ScrapeResult, error) {
	// Activity options for the lightweight selection activity.
	selectCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Select scrapers that need running.
	var selectResult SelectScrapersResult
	err := workflow.ExecuteActivity(selectCtx, (*Activities).SelectScrapers,
		SelectScrapersParams(params)).Get(ctx, &selectResult)
	if err != nil {
		return nil, fmt.Errorf("select scrapers: %w", err)
	}

	if len(selectResult.ScraperNames) == 0 {
		return &ScrapeResult{}, nil
	}

	// Track progress for query handler.
	progress := &ScrapeProgress{
		Total: len(selectResult.ScraperNames),
	}
	outcomes := make([]ScraperOutcome, 0, len(selectResult.ScraperNames))

	// Register query handler for scrape_progress.
	err = workflow.SetQueryHandler(ctx, "scrape_progress", func() (*ScrapeProgress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Fan out child workflows with max 5 concurrent.
	const maxConcurrent = 5
	sem := workflow.NewSemaphore(ctx, int64(maxConcurrent))

	for _, name := range selectResult.ScraperNames {
		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			var outcome ScraperOutcome
			outcome.Scraper = name

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("geo-scrape-%s-%s",
					name, workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var childResult ScrapeSingleResult
			err := workflow.ExecuteChildWorkflow(childCtx, ScrapeSingleWorkflow, ScrapeSingleParams{
				Scraper: name,
			}).Get(gCtx, &childResult)

			if err != nil {
				outcome.Status = "failed"
				outcome.Error = err.Error()
			} else {
				outcome.Status = "complete"
				outcome.RowsSynced = childResult.RowsSynced
			}

			// Direct append is safe — Temporal coroutines are cooperative (single-threaded).
			outcomes = append(outcomes, outcome)
			progress.Running--
			if outcome.Status == "complete" {
				progress.Completed++
			} else {
				progress.Failed++
			}
			progress.Outcomes = outcomes
		})
	}

	// Wait for all in-flight goroutines to complete.
	for i := 0; i < maxConcurrent; i++ {
		_ = sem.Acquire(ctx, 1)
	}

	result := &ScrapeResult{
		Outcomes: outcomes,
		Synced:   progress.Completed,
		Failed:   progress.Failed,
	}
	return result, nil
}

// ScrapeSingleWorkflow orchestrates the sync of a single geo scraper:
// StartSyncLog → SyncScraper → CompleteSyncLog/FailSyncLog.
func ScrapeSingleWorkflow(ctx workflow.Context, params ScrapeSingleParams) (*ScrapeSingleResult, error) {
	// Short-lived activity options for sync log operations.
	logCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Long-running activity options for the actual scraper sync.
	syncCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	// 1. Start sync log.
	var startResult StartSyncLogResult
	err := workflow.ExecuteActivity(logCtx, (*Activities).StartSyncLog,
		StartSyncLogParams(params)).Get(ctx, &startResult)
	if err != nil {
		return nil, fmt.Errorf("start sync log for %s: %w", params.Scraper, err)
	}

	// 2. Run the actual scraper sync.
	var syncResult SyncScraperResult
	syncErr := workflow.ExecuteActivity(syncCtx, (*Activities).SyncScraper,
		SyncScraperParams(params)).Get(ctx, &syncResult)

	// 3. Complete or fail the sync log.
	if syncErr != nil {
		_ = workflow.ExecuteActivity(logCtx, (*Activities).FailSyncLog, FailSyncLogParams{
			SyncID: startResult.SyncID,
			Error:  syncErr.Error(),
		}).Get(ctx, nil)
		return nil, fmt.Errorf("sync scraper %s: %w", params.Scraper, syncErr)
	}

	// Log completion failure is non-fatal — the sync itself succeeded.
	_ = workflow.ExecuteActivity(logCtx, (*Activities).CompleteSyncLog, CompleteSyncLogParams{
		SyncID:     startResult.SyncID,
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}).Get(ctx, nil)

	return &ScrapeSingleResult{
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}, nil
}
