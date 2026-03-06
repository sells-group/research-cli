package sdk

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// FanOutParams configures the generic fan-out.
type FanOutParams struct {
	Items            []string `json:"items"`
	Concurrency      int      `json:"concurrency,omitempty"` // default 5
	QueryName        string   `json:"query_name"`
	WorkflowIDPrefix string   `json:"workflow_id_prefix"`
	Full             bool     `json:"full,omitempty"`

	// PauseSignal and ResumeSignal enable pause/resume support.
	// When PauseSignal is received, the fan-out stops launching new items
	// until ResumeSignal is received. Both must be set to enable.
	PauseSignal  string `json:"pause_signal,omitempty"`
	ResumeSignal string `json:"resume_signal,omitempty"`

	// ChildParamsFn generates custom child workflow arguments for each item.
	// If nil, defaults to passing SyncItemParams{Name: item, Full: Full}.
	// The returned slice is spread as variadic args to ExecuteChildWorkflow.
	ChildParamsFn func(item string) []interface{}

	// ResultHandlerFn processes a completed child workflow future into an ItemOutcome.
	// If nil, defaults to deserializing as SyncItemResult.
	ResultHandlerFn func(item string, gCtx workflow.Context, f workflow.ChildWorkflowFuture) ItemOutcome
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

	// Set up pause/resume signal handling if configured.
	paused := false
	if params.PauseSignal != "" && params.ResumeSignal != "" {
		pauseCh := workflow.GetSignalChannel(ctx, params.PauseSignal)
		resumeCh := workflow.GetSignalChannel(ctx, params.ResumeSignal)

		workflow.Go(ctx, func(gCtx workflow.Context) {
			for {
				sel := workflow.NewSelector(gCtx)
				sel.AddReceive(pauseCh, func(ch workflow.ReceiveChannel, _ bool) {
					var signal interface{}
					ch.Receive(gCtx, &signal)
					paused = true
					progress.Paused = true
				})
				sel.AddReceive(resumeCh, func(ch workflow.ReceiveChannel, _ bool) {
					var signal interface{}
					ch.Receive(gCtx, &signal)
					paused = false
					progress.Paused = false
				})
				sel.Select(gCtx)
			}
		})
	}

	// Fan out child workflows with bounded concurrency.
	sem := workflow.NewSemaphore(ctx, int64(concurrency))

	for _, itemName := range params.Items {
		// Wait while paused.
		for paused {
			if sleepErr := workflow.Sleep(ctx, 5*time.Second); sleepErr != nil {
				return nil, fmt.Errorf("sleep during pause: %w", sleepErr)
			}
		}

		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("%s-%s-%s",
					params.WorkflowIDPrefix, itemName,
					workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var outcome ItemOutcome
			if params.ResultHandlerFn != nil {
				// Custom result handling: caller controls Get() and interpretation.
				var args []interface{}
				if params.ChildParamsFn != nil {
					args = params.ChildParamsFn(itemName)
				} else {
					args = []interface{}{SyncItemParams{Name: itemName, Full: params.Full}}
				}
				future := workflow.ExecuteChildWorkflow(childCtx, childFn, args...)
				outcome = params.ResultHandlerFn(itemName, gCtx, future)
			} else {
				// Default: SyncItemParams → SyncItemResult.
				var args []interface{}
				if params.ChildParamsFn != nil {
					args = params.ChildParamsFn(itemName)
				} else {
					args = []interface{}{SyncItemParams{Name: itemName, Full: params.Full}}
				}

				var childResult SyncItemResult
				err := workflow.ExecuteChildWorkflow(childCtx, childFn, args...).Get(gCtx, &childResult)
				outcome.Name = itemName
				if err != nil {
					outcome.Status = "failed"
					outcome.Error = err.Error()
				} else {
					outcome.Status = "complete"
					outcome.RowsSynced = childResult.RowsSynced
				}
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
