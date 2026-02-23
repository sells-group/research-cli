package model

import (
	"time"

	"github.com/sells-group/research-cli/pkg/ppp"
)

// RunStatus represents the current state of an enrichment run.
type RunStatus string

const (
	RunStatusQueued       RunStatus = "queued"
	RunStatusCrawling     RunStatus = "crawling"
	RunStatusClassifying  RunStatus = "classifying"
	RunStatusExtracting   RunStatus = "extracting"
	RunStatusAggregating  RunStatus = "aggregating"
	RunStatusWritingSF    RunStatus = "writing_sf"
	RunStatusComplete     RunStatus = "complete"
	RunStatusFailed       RunStatus = "failed"
)

// InputMode describes what data was available at pipeline start.
type InputMode string

const (
	InputModeURLOnly  InputMode = "url_only"   // Only URL
	InputModeMinimal  InputMode = "minimal"    // URL + Name
	InputModeStandard InputMode = "standard"   // URL + Name + Location
	InputModePreSeeded InputMode = "pre_seeded" // URL + Name + Location + PreSeeded
)

// Company represents a company to be enriched.
type Company struct {
	URL          string         `json:"url"`
	Name         string         `json:"name"`
	SalesforceID string         `json:"salesforce_id"`
	NotionPageID string         `json:"notion_page_id"`
	Location     string         `json:"location"`
	City         string         `json:"city,omitempty"`
	State        string         `json:"state,omitempty"`
	ZipCode      string         `json:"zip_code,omitempty"`
	Street       string         `json:"street,omitempty"`
	PreSeeded    map[string]any `json:"pre_seeded,omitempty"`  // CSV-sourced field values for gap-filling
	InputMode    InputMode      `json:"input_mode,omitempty"`  // Observability: what data was available at start
}

// Run represents a single enrichment run for a company.
type Run struct {
	ID        string    `json:"id"`
	Company   Company   `json:"company"`
	Status    RunStatus `json:"status"`
	Result    *RunResult `json:"result,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RunResult holds the final outcome of a run.
type RunResult struct {
	Score          float64            `json:"score"`
	FieldsFound    int                `json:"fields_found"`
	FieldsTotal    int                `json:"fields_total"`
	TotalTokens    int                `json:"total_tokens"`
	TotalCost      float64            `json:"total_cost"`
	Phases         []PhaseResult      `json:"phases"`
	Answers        []ExtractionAnswer `json:"answers"`
	Report         string             `json:"report"`
	SalesforceSync bool               `json:"salesforce_sync"`
	Error          string             `json:"error,omitempty"`
}

// RunPhase represents a phase within a run.
type RunPhase struct {
	ID     string      `json:"id"`
	RunID  string      `json:"run_id"`
	Name   string      `json:"name"`
	Status PhaseStatus `json:"status"`
	Result *PhaseResult `json:"result,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// PhaseStatus represents the current state of a pipeline phase.
type PhaseStatus string

const (
	PhaseStatusRunning   PhaseStatus = "running"
	PhaseStatusComplete  PhaseStatus = "complete"
	PhaseStatusFailed    PhaseStatus = "failed"
	PhaseStatusSkipped   PhaseStatus = "skipped"
)

// PhaseResult holds the outcome of a pipeline phase.
type PhaseResult struct {
	Name       string     `json:"name"`
	Status     PhaseStatus `json:"status"`
	Duration   int64      `json:"duration_ms"`
	TokenUsage TokenUsage `json:"token_usage"`
	Error      string     `json:"error,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// EnrichmentResult is the final output of the pipeline.
type EnrichmentResult struct {
	Company     Company               `json:"company"`
	RunID       string                `json:"run_id"`
	Score       float64               `json:"score"`
	Answers     []ExtractionAnswer    `json:"answers"`
	FieldValues map[string]FieldValue `json:"field_values"`
	PPPMatches  []ppp.LoanMatch       `json:"ppp_matches,omitempty"`
	Report      string                `json:"report"`
	Phases      []PhaseResult         `json:"phases"`
	TotalTokens int                   `json:"total_tokens"`
	TotalCost   float64               `json:"total_cost"`
}

// FieldValue is a resolved value ready for Salesforce.
type FieldValue struct {
	FieldKey   string     `json:"field_key"`
	SFField    string     `json:"sf_field"`
	Value      any        `json:"value"`
	Confidence float64    `json:"confidence"`
	Source     string     `json:"source"`
	Tier       int        `json:"tier"`
	DataAsOf   *time.Time `json:"data_as_of,omitempty"`
}

// Checkpoint stores intermediate pipeline state for resume after failure.
type Checkpoint struct {
	CompanyID string `json:"company_id"`
	Phase     string `json:"phase"`
	Data      []byte `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}
