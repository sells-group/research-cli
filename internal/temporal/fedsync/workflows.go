package fedsync

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/sells-group/research-cli/internal/temporal/sdk"
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
	selectCtx := workflow.WithActivityOptions(ctx, sdk.ShortActivityOptions())

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

	// Delegate to the shared fan-out pattern.
	fanResult, err := sdk.FanOut(ctx, sdk.FanOutParams{
		Items:            selectResult.DatasetNames,
		Concurrency:      5,
		QueryName:        "fedsync_progress",
		WorkflowIDPrefix: "dataset-sync",
		Full:             params.Full,
	}, DatasetSyncWorkflow)
	if err != nil {
		return nil, err
	}

	// Convert SDK outcomes to domain types.
	outcomes := make([]DatasetOutcome, len(fanResult.Outcomes))
	for i, o := range fanResult.Outcomes {
		outcomes[i] = DatasetOutcome{
			Dataset:    o.Name,
			Status:     o.Status,
			RowsSynced: o.RowsSynced,
			Error:      o.Error,
		}
	}

	// Check for overdue datasets and include in notification.
	lagCtx := workflow.WithActivityOptions(ctx, sdk.ShortActivityOptions())
	var lagResult SyncLagResult
	lagErr := workflow.ExecuteActivity(lagCtx, (*Activities).CheckSyncLag).Get(ctx, &lagResult)

	overdueMsg := ""
	if lagErr == nil && len(lagResult.Overdue) > 0 {
		overdueMsg = fmt.Sprintf(" (%d overdue datasets)", len(lagResult.Overdue))
	}

	// Send completion notification.
	notifyCtx := workflow.WithActivityOptions(ctx, sdk.ShortActivityOptions())
	_ = workflow.ExecuteActivity(notifyCtx, (*Activities).NotifyComplete, sdk.NotifyParams{
		Domain:  "fedsync",
		Synced:  fanResult.Synced,
		Failed:  fanResult.Failed,
		Total:   len(selectResult.DatasetNames),
		Message: fmt.Sprintf("Fedsync run: %d synced, %d failed out of %d%s", fanResult.Synced, fanResult.Failed, len(selectResult.DatasetNames), overdueMsg),
	}).Get(ctx, nil)

	return &RunResult{
		Outcomes: outcomes,
		Synced:   fanResult.Synced,
		Failed:   fanResult.Failed,
	}, nil
}

// DatasetSyncParams is the input for DatasetSyncWorkflow.
type DatasetSyncParams = sdk.SyncItemParams

// DatasetSyncResult is the output of DatasetSyncWorkflow.
type DatasetSyncResult = sdk.SyncItemResult

// DatasetSyncWorkflow orchestrates the sync of a single dataset:
// StartSyncLog → SyncDataset → CompleteSyncLog/FailSyncLog.
func DatasetSyncWorkflow(ctx workflow.Context, params DatasetSyncParams) (*DatasetSyncResult, error) {
	logCtx := workflow.WithActivityOptions(ctx, sdk.LogActivityOptions())

	// Determine timeout based on dataset type.
	syncTimeout := 60 * time.Minute
	ocrHeavy := map[string]bool{
		"adv_part2":      true,
		"adv_part3":      true,
		"adv_enrichment": true,
		"adv_extract":    true,
	}
	if ocrHeavy[params.Name] {
		syncTimeout = 120 * time.Minute
	}

	// Long-running activity options for the actual dataset sync.
	syncCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: syncTimeout,
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
	err := workflow.ExecuteActivity(logCtx, (*Activities).StartSyncLog, sdk.StartSyncLogParams{
		Name: params.Name,
	}).Get(ctx, &startResult)
	if err != nil {
		return nil, fmt.Errorf("start sync log for %s: %w", params.Name, err)
	}

	// 2. Run the actual dataset sync.
	var syncResult SyncDatasetResult
	syncErr := workflow.ExecuteActivity(syncCtx, (*Activities).SyncDataset, SyncDatasetParams{
		Dataset: params.Name,
		Full:    params.Full,
	}).Get(ctx, &syncResult)

	// 3. Complete or fail the sync log.
	if syncErr != nil {
		_ = workflow.ExecuteActivity(logCtx, (*Activities).FailSyncLog, sdk.FailSyncLogParams{
			SyncID: startResult.SyncID,
			Error:  syncErr.Error(),
		}).Get(ctx, nil)
		return nil, fmt.Errorf("sync dataset %s: %w", params.Name, syncErr)
	}

	// Log completion failure is non-fatal — the sync itself succeeded.
	_ = workflow.ExecuteActivity(logCtx, (*Activities).CompleteSyncLog, sdk.CompleteSyncLogParams{
		SyncID:     startResult.SyncID,
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}).Get(ctx, nil)

	return &DatasetSyncResult{
		RowsSynced: syncResult.RowsSynced,
		Metadata:   syncResult.Metadata,
	}, nil
}
