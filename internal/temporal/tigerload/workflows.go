// Package tigerload provides Temporal workflows and activities for TIGER/Line data loading.
package tigerload

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Params is the input for Workflow.
type Params struct {
	Year        int      `json:"year"`
	States      []string `json:"states,omitempty"`      // empty = all 51
	Tables      []string `json:"tables,omitempty"`      // empty = all 9
	Concurrency int      `json:"concurrency,omitempty"` // default 3
	Incremental bool     `json:"incremental"`
}

// Result is the output of Workflow.
type Result struct {
	Outcomes []StateOutcome `json:"outcomes"`
	National int            `json:"national"`
	Loaded   int            `json:"loaded"`
	Failed   int            `json:"failed"`
}

// StateOutcome records the result of loading a single state's products.
type StateOutcome struct {
	State      string `json:"state"`
	Status     string `json:"status"` // "complete", "failed", "skipped"
	RowsLoaded int64  `json:"rows_loaded,omitempty"`
	Error      string `json:"error,omitempty"`
}

// Progress is returned by the tiger_load_progress query.
type Progress struct {
	TotalStates int            `json:"total_states"`
	Completed   int            `json:"completed"`
	Failed      int            `json:"failed"`
	Running     int            `json:"running"`
	Outcomes    []StateOutcome `json:"outcomes"`
}

// TigerStateParams is the input for TigerStateWorkflow.
type TigerStateParams struct {
	State       string   `json:"state"`
	FIPS        string   `json:"fips"`
	Year        int      `json:"year"`
	Products    []string `json:"products"`
	Incremental bool     `json:"incremental"`
}

// TigerStateResult is the output of TigerStateWorkflow.
type TigerStateResult struct {
	RowsLoaded int64 `json:"rows_loaded"`
}

// Workflow orchestrates the full TIGER data load:
// 1. Create parent + per-state tables (DDL must precede COPY)
// 2. Load national products into the parent tables
// 3. Fan out child workflows per state (with concurrency semaphore)
// 4. Populate lookup tables
func Workflow(ctx workflow.Context, params Params) (*Result, error) {
	if params.Concurrency <= 0 {
		params.Concurrency = 3
	}

	// Long-running activity options for national/lookup loads.
	longCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	// Short activity options for DDL operations.
	shortCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// 1. Create parent + per-state tables (DDL must precede COPY).
	err := workflow.ExecuteActivity(shortCtx, (*Activities).CreateAllStateTables, CreateStateTablesParams{
		States: params.States,
		Tables: params.Tables,
	}).Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("create state tables: %w", err)
	}

	// 2. Load national products into the parent tables.
	var nationalResult LoadNationalResult
	err = workflow.ExecuteActivity(longCtx, (*Activities).LoadNational, LoadNationalParams{
		Year:        params.Year,
		Tables:      params.Tables,
		Incremental: params.Incremental,
	}).Get(ctx, &nationalResult)
	if err != nil {
		return nil, fmt.Errorf("load national products: %w", err)
	}

	// 3. Resolve state list for progress tracking.
	var resolveResult ResolveStatesResult
	err = workflow.ExecuteActivity(shortCtx, (*Activities).ResolveStates, ResolveStatesParams{
		States: params.States,
	}).Get(ctx, &resolveResult)
	if err != nil {
		return nil, fmt.Errorf("resolve states: %w", err)
	}

	// Track progress for query handler.
	progress := &Progress{
		TotalStates: len(resolveResult.States),
	}
	outcomes := make([]StateOutcome, 0, len(resolveResult.States))

	err = workflow.SetQueryHandler(ctx, "tiger_load_progress", func() (*Progress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Fan out child workflows with semaphore.
	sem := workflow.NewSemaphore(ctx, int64(params.Concurrency))

	for _, st := range resolveResult.States {
		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			var outcome StateOutcome
			outcome.State = st.Abbr

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("tiger-state-%s-%s",
					st.Abbr, workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var childResult TigerStateResult
			err := workflow.ExecuteChildWorkflow(childCtx, TigerStateWorkflow, TigerStateParams{
				State:       st.Abbr,
				FIPS:        st.FIPS,
				Year:        params.Year,
				Products:    params.Tables,
				Incremental: params.Incremental,
			}).Get(gCtx, &childResult)

			if err != nil {
				outcome.Status = "failed"
				outcome.Error = err.Error()
			} else {
				outcome.Status = "complete"
				outcome.RowsLoaded = childResult.RowsLoaded
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
	for i := 0; i < params.Concurrency; i++ {
		_ = sem.Acquire(ctx, 1)
	}

	// 4. Populate lookup tables.
	err = workflow.ExecuteActivity(longCtx, (*Activities).PopulateLookups).Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("populate lookups: %w", err)
	}

	result := &Result{
		Outcomes: outcomes,
		National: nationalResult.Loaded,
		Loaded:   progress.Completed,
		Failed:   progress.Failed,
	}
	return result, nil
}

// TigerStateWorkflow loads all per-state products for a single state sequentially.
// Per-county products (EDGES, FACES, ADDR, FEATNAMES) download one file per county,
// so large states like Texas (254 counties × 4 products) can take several hours.
func TigerStateWorkflow(ctx workflow.Context, params TigerStateParams) (*TigerStateResult, error) {
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 8 * time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var totalRows int64
	var productResult LoadStateProductResult
	err := workflow.ExecuteActivity(actCtx, (*Activities).LoadStateProducts,
		LoadStateProductsParams(params)).Get(ctx, &productResult)
	if err != nil {
		return nil, fmt.Errorf("load products for %s: %w", params.State, err)
	}
	totalRows += productResult.RowsLoaded

	return &TigerStateResult{RowsLoaded: totalRows}, nil
}
