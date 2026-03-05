package fedsync

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// RunParams is the input for RunWorkflow.
type RunParams struct {
	Phase    *string  `json:"phase,omitempty"`
	Datasets []string `json:"datasets,omitempty"`
	Force    bool     `json:"force"`
	Full     bool     `json:"full"`
}

// DatasetOutcome records the result of a single dataset sync.
type DatasetOutcome struct {
	Dataset    string `json:"dataset"`
	Status     string `json:"status"` // "complete", "failed", "skipped"
	RowsSynced int64  `json:"rows_synced,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RunResult is the output of RunWorkflow.
type RunResult struct {
	Outcomes []DatasetOutcome `json:"outcomes"`
	Synced   int              `json:"synced"`
	Failed   int              `json:"failed"`
}

// Progress is returned by the fedsync_progress query.
type Progress struct {
	Total     int              `json:"total"`
	Completed int              `json:"completed"`
	Failed    int              `json:"failed"`
	Running   int              `json:"running"`
	Outcomes  []DatasetOutcome `json:"outcomes"`
}

// RunWorkflow orchestrates the sync of selected federal datasets.
// It selects datasets, then fans out DatasetSyncWorkflow child workflows
// with a max concurrency of 5.
func RunWorkflow(ctx workflow.Context, params RunParams) (*RunResult, error) {
	// Activity options for the lightweight selection activity.
	selectCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Select datasets that need syncing.
	var selectResult SelectDatasetsResult
	err := workflow.ExecuteActivity(selectCtx, (*Activities).SelectDatasets, SelectDatasetsParams{
		Phase:    params.Phase,
		Datasets: params.Datasets,
		Force:    params.Force,
	}).Get(ctx, &selectResult)
	if err != nil {
		return nil, fmt.Errorf("select datasets: %w", err)
	}

	if len(selectResult.DatasetNames) == 0 {
		return &RunResult{}, nil
	}

	// Track progress for query handler.
	progress := &Progress{
		Total: len(selectResult.DatasetNames),
	}
	outcomes := make([]DatasetOutcome, 0, len(selectResult.DatasetNames))

	// Register query handler for fedsync_progress.
	err = workflow.SetQueryHandler(ctx, "fedsync_progress", func() (*Progress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Fan out child workflows with max 5 concurrent.
	const maxConcurrent = 5
	sem := workflow.NewSemaphore(ctx, int64(maxConcurrent))

	for _, dsName := range selectResult.DatasetNames {
		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			var outcome DatasetOutcome
			outcome.Dataset = dsName

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("dataset-sync-%s-%s",
					dsName, workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var childResult DatasetSyncResult
			err := workflow.ExecuteChildWorkflow(childCtx, DatasetSyncWorkflow, DatasetSyncParams{
				Dataset: dsName,
				Full:    params.Full,
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

	result := &RunResult{
		Outcomes: outcomes,
		Synced:   progress.Completed,
		Failed:   progress.Failed,
	}
	return result, nil
}

// DatasetSyncParams is the input for DatasetSyncWorkflow.
type DatasetSyncParams struct {
	Dataset string `json:"dataset"`
	Full    bool   `json:"full"`
}

// DatasetSyncResult is the output of DatasetSyncWorkflow.
type DatasetSyncResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// DatasetSyncWorkflow orchestrates the sync of a single dataset:
// StartSyncLog → SyncDataset → CompleteSyncLog/FailSyncLog.
func DatasetSyncWorkflow(ctx workflow.Context, params DatasetSyncParams) (*DatasetSyncResult, error) {
	// Short-lived activity options for sync log operations.
	logCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Long-running activity options for the actual dataset sync.
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
	err := workflow.ExecuteActivity(logCtx, (*Activities).StartSyncLog, StartSyncLogParams{
		Dataset: params.Dataset,
	}).Get(ctx, &startResult)
	if err != nil {
		return nil, fmt.Errorf("start sync log for %s: %w", params.Dataset, err)
	}

	// 2. Run the actual dataset sync.
	var syncResult SyncDatasetResult
	syncErr := workflow.ExecuteActivity(syncCtx, (*Activities).SyncDataset, SyncDatasetParams(params)).Get(ctx, &syncResult)

	// 3. Complete or fail the sync log.
	if syncErr != nil {
		_ = workflow.ExecuteActivity(logCtx, (*Activities).FailSyncLog, FailSyncLogParams{
			SyncID: startResult.SyncID,
			Error:  syncErr.Error(),
		}).Get(ctx, nil)
		return nil, fmt.Errorf("sync dataset %s: %w", params.Dataset, syncErr)
	}

	// Log completion failure is non-fatal — the sync itself succeeded.
	_ = workflow.ExecuteActivity(logCtx, (*Activities).CompleteSyncLog, CompleteSyncLogParams{
		SyncID:     startResult.SyncID,
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}).Get(ctx, nil)

	return &DatasetSyncResult{
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}, nil
}
