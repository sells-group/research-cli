package sdk

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// FanOutParams configures the generic fan-out.
type FanOutParams struct {
	Items            []string `json:"items"`
	Concurrency      int      `json:"concurrency,omitempty"` // default 5
	QueryName        string   `json:"query_name"`
	WorkflowIDPrefix string   `json:"workflow_id_prefix"`
	Full             bool     `json:"full,omitempty"`
}

// ChildWorkflowFn is the signature for fan-out child workflows.
type ChildWorkflowFn func(ctx workflow.Context, params SyncItemParams) (*SyncItemResult, error)

// FanOut runs child workflows with bounded concurrency and progress tracking.
// It registers a query handler under params.QueryName and launches one child
// workflow per item, collecting outcomes into a FanOutResult.
func FanOut(ctx workflow.Context, params FanOutParams, childFn interface{}) (*FanOutResult, error) {
	if len(params.Items) == 0 {
		return &FanOutResult{}, nil
	}

	concurrency := params.Concurrency
	if concurrency <= 0 {
		concurrency = 5
	}

	// Track progress for query handler.
	progress := &FanOutProgress{
		Total: len(params.Items),
	}
	outcomes := make([]ItemOutcome, 0, len(params.Items))

	// Register query handler.
	err := workflow.SetQueryHandler(ctx, params.QueryName, func() (*FanOutProgress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Fan out child workflows with bounded concurrency.
	sem := workflow.NewSemaphore(ctx, int64(concurrency))

	for _, itemName := range params.Items {
		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			var outcome ItemOutcome
			outcome.Name = itemName

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("%s-%s-%s",
					params.WorkflowIDPrefix, itemName,
					workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var childResult SyncItemResult
			err := workflow.ExecuteChildWorkflow(childCtx, childFn, SyncItemParams{
				Name: itemName,
				Full: params.Full,
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
	for i := 0; i < concurrency; i++ {
		_ = sem.Acquire(ctx, 1)
	}

	return &FanOutResult{
		Outcomes: outcomes,
		Synced:   progress.Completed,
		Failed:   progress.Failed,
	}, nil
}
