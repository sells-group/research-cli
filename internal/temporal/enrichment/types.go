// Package enrichment provides Temporal workflows and activities for the enrichment pipeline.
package enrichment

import "github.com/sells-group/research-cli/internal/model"

// EnrichCompanyParams is the input for EnrichCompanyWorkflow.
type EnrichCompanyParams struct {
	Company model.Company `json:"company"`
}

// EnrichCompanyResult is the output of EnrichCompanyWorkflow.
type EnrichCompanyResult struct {
	Score   float64 `json:"score"`
	Answers int     `json:"answers"`
	Error   string  `json:"error,omitempty"`
}

// BatchEnrichParams is the input for BatchEnrichWorkflow.
type BatchEnrichParams struct {
	Companies   []model.Company `json:"companies"`
	Concurrency int             `json:"concurrency"`
}

// BatchEnrichResult is the output of BatchEnrichWorkflow.
type BatchEnrichResult struct {
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

// BatchProgress is returned by the batch_progress query.
type BatchProgress struct {
	Total     int  `json:"total"`
	Completed int  `json:"completed"`
	Failed    int  `json:"failed"`
	Running   int  `json:"running"`
	Paused    bool `json:"paused"`
}

// PhaseResult captures the outcome of a single pipeline phase.
type PhaseResult struct {
	Phase  string `json:"phase"`
	Status string `json:"status"` // "success", "failed", "skipped"
	Error  string `json:"error,omitempty"`
}

// Progress is returned by the enrichment_progress query.
type Progress struct {
	Company      string        `json:"company"`
	CurrentPhase string        `json:"current_phase"`
	Phases       []PhaseResult `json:"phases"`
}
