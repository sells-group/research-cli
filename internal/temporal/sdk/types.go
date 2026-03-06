// Package sdk provides shared types and reusable workflow patterns for Temporal integrations.
package sdk

// ItemOutcome records the result of syncing a single item.
type ItemOutcome struct {
	Name       string `json:"name"`
	Status     string `json:"status"` // "complete", "failed"
	RowsSynced int64  `json:"rows_synced,omitempty"`
	Error      string `json:"error,omitempty"`
}

// FanOutResult is the output of any fan-out workflow.
type FanOutResult struct {
	Outcomes []ItemOutcome `json:"outcomes"`
	Synced   int           `json:"synced"`
	Failed   int           `json:"failed"`
}

// FanOutProgress is returned by progress query handlers.
type FanOutProgress struct {
	Total     int           `json:"total"`
	Completed int           `json:"completed"`
	Failed    int           `json:"failed"`
	Running   int           `json:"running"`
	Outcomes  []ItemOutcome `json:"outcomes"`
}

// SyncItemParams is the input for the generic sync child workflow.
type SyncItemParams struct {
	Name     string         `json:"name"`
	Full     bool           `json:"full,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SyncItemResult is the output of the generic sync child workflow.
type SyncItemResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}
