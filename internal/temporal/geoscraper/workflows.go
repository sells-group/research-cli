// Package geoscraper provides Temporal workflows and activities for geo data scraping.
package geoscraper

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/sells-group/research-cli/internal/temporal/sdk"
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
type ScrapeSingleParams = sdk.SyncItemParams

// ScrapeSingleResult is the output of ScrapeSingleWorkflow.
type ScrapeSingleResult = sdk.SyncItemResult

// ScrapeWorkflow orchestrates the sync of selected geo scrapers.
// It selects scrapers, then fans out ScrapeSingleWorkflow child workflows
// with a max concurrency of 5.
func ScrapeWorkflow(ctx workflow.Context, params ScrapeParams) (*ScrapeResult, error) {
	// Activity options for the lightweight selection activity.
	selectCtx := workflow.WithActivityOptions(ctx, sdk.ShortActivityOptions())

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

	// Delegate to the shared fan-out pattern.
	fanResult, err := sdk.FanOut(ctx, sdk.FanOutParams{
		Items:            selectResult.ScraperNames,
		Concurrency:      5,
		QueryName:        "scrape_progress",
		WorkflowIDPrefix: "geo-scrape",
	}, ScrapeSingleWorkflow)
	if err != nil {
		return nil, err
	}

	// Convert SDK outcomes to domain types.
	outcomes := make([]ScraperOutcome, len(fanResult.Outcomes))
	for i, o := range fanResult.Outcomes {
		outcomes[i] = ScraperOutcome{
			Scraper:    o.Name,
			Status:     o.Status,
			RowsSynced: o.RowsSynced,
			Error:      o.Error,
		}
	}

	return &ScrapeResult{
		Outcomes: outcomes,
		Synced:   fanResult.Synced,
		Failed:   fanResult.Failed,
	}, nil
}

// ScrapeSingleWorkflow orchestrates the sync of a single geo scraper:
// StartSyncLog → SyncScraper → CompleteSyncLog/FailSyncLog.
func ScrapeSingleWorkflow(ctx workflow.Context, params ScrapeSingleParams) (*ScrapeSingleResult, error) {
	logCtx := workflow.WithActivityOptions(ctx, sdk.LogActivityOptions())

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
	var startResult sdk.StartSyncLogResult
	err := workflow.ExecuteActivity(logCtx, (*Activities).StartSyncLog,
		sdk.StartSyncLogParams{Name: params.Name}).Get(ctx, &startResult)
	if err != nil {
		return nil, fmt.Errorf("start sync log for %s: %w", params.Name, err)
	}

	// 2. Run the actual scraper sync.
	var syncResult SyncScraperResult
	syncErr := workflow.ExecuteActivity(syncCtx, (*Activities).SyncScraper,
		SyncScraperParams{Scraper: params.Name}).Get(ctx, &syncResult)

	// 3. Complete or fail the sync log.
	if syncErr != nil {
		_ = workflow.ExecuteActivity(logCtx, (*Activities).FailSyncLog, sdk.FailSyncLogParams{
			SyncID: startResult.SyncID,
			Error:  syncErr.Error(),
		}).Get(ctx, nil)
		return nil, fmt.Errorf("sync scraper %s: %w", params.Name, syncErr)
	}

	// Log completion failure is non-fatal — the sync itself succeeded.
	_ = workflow.ExecuteActivity(logCtx, (*Activities).CompleteSyncLog, sdk.CompleteSyncLogParams{
		SyncID:     startResult.SyncID,
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}).Get(ctx, nil)

	return &ScrapeSingleResult{
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}, nil
}
