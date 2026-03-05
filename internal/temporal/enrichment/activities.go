package enrichment

import (
	"context"

	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/resilience"
)

// Activities holds dependencies for enrichment Temporal activities.
// It wraps the existing pipeline to run enrichment as a coarse-grained activity.
type Activities struct {
	pipeline *pipeline.Pipeline
}

// NewActivities creates enrichment Activities.
func NewActivities(p *pipeline.Pipeline) *Activities {
	return &Activities{pipeline: p}
}

// RunEnrichmentParams is the input for RunEnrichment.
type RunEnrichmentParams struct {
	Company model.Company `json:"company"`
}

// RunEnrichmentResult is the output of RunEnrichment.
type RunEnrichmentResult struct {
	Score   float64 `json:"score"`
	Answers int     `json:"answers"`
}

// RunEnrichment executes the full 9-phase enrichment pipeline for a single company.
// This is a coarse-grained activity that wraps pipeline.Run() — no rewrite of
// pipeline internals. The pipeline handles all phase orchestration internally.
func (a *Activities) RunEnrichment(ctx context.Context, params RunEnrichmentParams) (*RunEnrichmentResult, error) {
	log := zap.L().With(zap.String("company", params.Company.URL))
	log.Info("starting enrichment via Temporal activity")

	result, err := a.pipeline.Run(ctx, params.Company)
	if err != nil {
		// Classify error for Temporal retry decisions.
		if !resilience.IsTransient(err) {
			return nil, temporal.NewNonRetryableApplicationError(
				err.Error(), "PermanentError", err)
		}
		return nil, eris.Wrap(err, "enrichment failed")
	}

	return &RunEnrichmentResult{
		Score:   result.Score,
		Answers: len(result.Answers),
	}, nil
}

// FlushSFWritesParams is the input for FlushSFWrites.
type FlushSFWritesParams struct {
	// Write intents are collected in the workflow state and serialized here.
	// In practice, the workflow passes them via side effect or as serialized data.
	IntentCount int `json:"intent_count"`
}

// FlushSFWritesResult is the output of FlushSFWrites.
type FlushSFWritesResult struct {
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}
