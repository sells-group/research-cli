package enrichment

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// EnrichCompanyWorkflow orchestrates the enrichment of a single company.
// It wraps the pipeline as a coarse-grained activity to avoid rewriting
// all 9 phases as separate activities (future optimization).
func EnrichCompanyWorkflow(ctx workflow.Context, params EnrichCompanyParams) (*EnrichCompanyResult, error) {
	progress := &Progress{
		Company:      params.Company.URL,
		CurrentPhase: "starting",
	}

	// Register query handler.
	err := workflow.SetQueryHandler(ctx, "enrichment_progress", func() (*Progress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Execute the full pipeline as a single activity.
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2.0,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	progress.CurrentPhase = "enriching"

	var result RunEnrichmentResult
	err = workflow.ExecuteActivity(actCtx, (*Activities).RunEnrichment, RunEnrichmentParams(params)).Get(ctx, &result)

	if err != nil {
		progress.CurrentPhase = "failed"
		progress.Phases = append(progress.Phases, PhaseResult{
			Phase:  "pipeline",
			Status: "failed",
			Error:  err.Error(),
		})
		return &EnrichCompanyResult{
			Error: err.Error(),
		}, err
	}

	progress.CurrentPhase = "complete"
	progress.Phases = append(progress.Phases, PhaseResult{
		Phase:  "pipeline",
		Status: "success",
	})

	return &EnrichCompanyResult{
		Score:   result.Score,
		Answers: result.Answers,
	}, nil
}

// BatchEnrichWorkflow fans out EnrichCompanyWorkflow child workflows
// with bounded concurrency. Supports pause/resume signals.
func BatchEnrichWorkflow(ctx workflow.Context, params BatchEnrichParams) (*BatchEnrichResult, error) {
	if params.Concurrency <= 0 {
		params.Concurrency = 15
	}

	progress := &BatchProgress{
		Total: len(params.Companies),
	}
	paused := false

	// Register query handler.
	err := workflow.SetQueryHandler(ctx, "batch_progress", func() (*BatchProgress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Register signal handlers for pause/resume.
	pauseCh := workflow.GetSignalChannel(ctx, "pause_batch")
	resumeCh := workflow.GetSignalChannel(ctx, "resume_batch")

	// Handle signals in a goroutine.
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

	// Fan out with bounded concurrency.
	sem := workflow.NewSemaphore(ctx, int64(params.Concurrency))
	resultCh := workflow.NewChannel(ctx)

	for _, company := range params.Companies {
		// Check for pause.
		for paused {
			if err := workflow.Sleep(ctx, 5*time.Second); err != nil {
				return nil, fmt.Errorf("sleep during pause: %w", err)
			}
		}

		_ = sem.Acquire(ctx, 1)
		progress.Running++

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer sem.Release(1)

			childCtx := workflow.WithChildOptions(gCtx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("enrich-%s-%s",
					company.URL, workflow.GetInfo(gCtx).WorkflowExecution.RunID),
			})

			var childResult EnrichCompanyResult
			err := workflow.ExecuteChildWorkflow(childCtx, EnrichCompanyWorkflow, EnrichCompanyParams{
				Company: company,
			}).Get(gCtx, &childResult)

			success := err == nil && childResult.Error == ""
			resultCh.Send(gCtx, success)
		})
	}

	// Collect results.
	result := &BatchEnrichResult{}
	for range params.Companies {
		var success bool
		resultCh.Receive(ctx, &success)

		progress.Running--
		if success {
			progress.Completed++
			result.Succeeded++
		} else {
			progress.Failed++
			result.Failed++
		}
	}

	return result, nil
}
