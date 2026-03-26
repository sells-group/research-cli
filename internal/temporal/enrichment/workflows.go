package enrichment

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/temporal/sdk"
)

// EnrichCompanyWorkflow orchestrates the enrichment of a single company.
// It wraps the pipeline as a coarse-grained activity to avoid rewriting
// all 9 phases as separate activities (future optimization).
func EnrichCompanyWorkflow(ctx workflow.Context, params EnrichCompanyParams) (*EnrichCompanyResult, error) {
	workflow.GetLogger(ctx).Info("starting enrichment workflow",
		"company", params.Company.URL,
		"request_id", params.Metadata.RequestID,
		"trigger_source", params.Metadata.TriggerSource,
		"original_run_id", params.Metadata.OriginalRunID,
		"dedupe_key", params.Metadata.DedupeKey,
	)

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
// with bounded concurrency. Supports pause/resume signals via the SDK FanOut.
func BatchEnrichWorkflow(ctx workflow.Context, params BatchEnrichParams) (*BatchEnrichResult, error) {
	if params.Concurrency <= 0 {
		params.Concurrency = 15
	}

	// Build item list and company lookup from the Companies slice.
	items := make([]string, len(params.Companies))
	companyMap := make(map[string]model.Company, len(params.Companies))
	for i, c := range params.Companies {
		items[i] = c.URL
		companyMap[c.URL] = c
	}

	result, err := sdk.FanOut(ctx, sdk.FanOutParams{
		Items:            items,
		Concurrency:      params.Concurrency,
		QueryName:        "batch_progress",
		WorkflowIDPrefix: "enrich",
		PauseSignal:      "pause_batch",
		ResumeSignal:     "resume_batch",
		ChildParamsFn: func(item string) []interface{} {
			return []interface{}{EnrichCompanyParams{Company: companyMap[item]}}
		},
		ResultHandlerFn: func(item string, gCtx workflow.Context, f workflow.ChildWorkflowFuture) sdk.ItemOutcome {
			var childResult EnrichCompanyResult
			err := f.Get(gCtx, &childResult)
			if err != nil {
				return sdk.ItemOutcome{Name: item, Status: "failed", Error: err.Error()}
			}
			if childResult.Error != "" {
				return sdk.ItemOutcome{Name: item, Status: "failed", Error: childResult.Error}
			}
			return sdk.ItemOutcome{Name: item, Status: "complete"}
		},
	}, EnrichCompanyWorkflow)
	if err != nil {
		return nil, err
	}

	return &BatchEnrichResult{
		Succeeded: result.Synced,
		Failed:    result.Failed,
	}, nil
}
