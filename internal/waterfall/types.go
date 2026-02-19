package waterfall

import "time"

// SourceValue represents a field value from a specific source.
type SourceValue struct {
	Source              string     `json:"source"`
	Value               any        `json:"value"`
	RawConfidence       float64    `json:"raw_confidence"`
	EffectiveConfidence float64    `json:"effective_confidence"`
	DataAsOf            *time.Time `json:"data_as_of,omitempty"`
	Tier                int        `json:"tier"`
}

// FieldResolution is the outcome of waterfall evaluation for a single field.
type FieldResolution struct {
	FieldKey       string        `json:"field_key"`
	Resolved       bool          `json:"resolved"`
	Winner         *SourceValue  `json:"winner,omitempty"`
	Threshold      float64       `json:"threshold"`
	ThresholdMet   bool          `json:"threshold_met"`
	Attempts       []SourceValue `json:"attempts"`
	PremiumCostUSD float64       `json:"premium_cost_usd,omitempty"`
}

// WaterfallResult is the overall output of running the waterfall for a company.
type WaterfallResult struct {
	Resolutions     map[string]FieldResolution `json:"resolutions"`
	TotalPremiumUSD float64                    `json:"total_premium_usd"`
	FieldsResolved  int                        `json:"fields_resolved"`
	FieldsTotal     int                        `json:"fields_total"`
}
