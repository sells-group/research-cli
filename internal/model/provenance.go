package model

import "time"

// ProvenanceAttempt records a single extraction attempt for a field.
type ProvenanceAttempt struct {
	Source     string     `json:"source"`
	SourceURL  string     `json:"source_url,omitempty"`
	Value      any        `json:"value"`
	Confidence float64    `json:"confidence"`
	Tier       int        `json:"tier"`
	Reasoning  string     `json:"reasoning,omitempty"`
	DataAsOf   *time.Time `json:"data_as_of,omitempty"`
}

// FieldProvenance tracks per-field per-run audit trail of value provenance.
type FieldProvenance struct {
	ID                  int64               `json:"id,omitempty"`
	RunID               string              `json:"run_id"`
	CompanyURL          string              `json:"company_url"`
	FieldKey            string              `json:"field_key"`
	WinnerSource        string              `json:"winner_source"`
	WinnerValue         string              `json:"winner_value"`
	RawConfidence       float64             `json:"raw_confidence"`
	EffectiveConfidence float64             `json:"effective_confidence"`
	DataAsOf            *time.Time          `json:"data_as_of,omitempty"`
	Threshold           float64             `json:"threshold"`
	ThresholdMet        bool                `json:"threshold_met"`
	Attempts            []ProvenanceAttempt `json:"attempts"`
	PremiumCostUSD      float64             `json:"premium_cost_usd"`
	PreviousValue       string              `json:"previous_value,omitempty"`
	PreviousRunID       string              `json:"previous_run_id,omitempty"`
	ValueChanged        bool                `json:"value_changed"`
	CreatedAt           time.Time           `json:"created_at"`
}
