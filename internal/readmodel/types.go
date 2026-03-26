package readmodel

import "time"

// CompaniesFilter defines list companies pagination and search criteria.
type CompaniesFilter struct {
	Search string
	Limit  int
	Offset int
}

// CompanyGeoPoint is a mappable company address with enrichment metadata.
type CompanyGeoPoint struct {
	AddressID       int64
	CompanyID       int64
	Name            string
	Domain          string
	Street          string
	City            string
	State           string
	ZipCode         string
	Latitude        float64
	Longitude       float64
	EnrichmentScore *float64
}

// DatasetStatus is the read model for a single dataset status row.
type DatasetStatus struct {
	Name       string         `json:"name"`
	Table      string         `json:"table"`
	Phase      string         `json:"phase"`
	Cadence    string         `json:"cadence"`
	LastSync   *time.Time     `json:"last_sync,omitempty"`
	LastStatus string         `json:"last_status"`
	RowsSynced int64          `json:"rows_synced"`
	RowCount   int64          `json:"row_count"`
	NextDue    *time.Time     `json:"next_due,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// TableColumn describes a data explorer column.
type TableColumn struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Type     string `json:"type"`
	Sortable bool   `json:"sortable"`
}

// TableMeta describes a data explorer table.
type TableMeta struct {
	ID                string        `json:"id"`
	Name              string        `json:"name"`
	Category          string        `json:"category"`
	EstimatedRowCount int64         `json:"estimated_row_count"`
	Columns           []TableColumn `json:"columns"`
}

// DataQuerySort echoes the active sort in a data explorer response.
type DataQuerySort struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}

// DataQueryFilter echoes the active filter in a data explorer response.
type DataQueryFilter struct {
	Column string `json:"column"`
	Value  string `json:"value"`
}

// DataQueryParams defines a data explorer table query.
type DataQueryParams struct {
	Table         string
	Limit         int
	Offset        int
	SortColumn    string
	SortDirection string
	SearchColumn  string
	SearchValue   string
}

// DataQueryResult holds a paginated table query response.
type DataQueryResult struct {
	Rows      []map[string]any `json:"rows"`
	TotalRows int64            `json:"total_rows"`
	Page      int              `json:"page"`
	PageSize  int              `json:"page_size"`
	Sort      *DataQuerySort   `json:"sort,omitempty"`
	Filter    *DataQueryFilter `json:"filter,omitempty"`
}

// DataAggregateParams defines an aggregate query for the data explorer.
type DataAggregateParams struct {
	Table       string
	GroupBy     string
	Aggregation string
	ValueField  string
	Limit       int
}

// DataAggregateRow represents one grouped aggregate row.
type DataAggregateRow struct {
	Key   any `json:"key"`
	Value any `json:"value"`
}

// DataAggregateResult is the normalized aggregate response payload.
type DataAggregateResult struct {
	Table       string             `json:"table"`
	GroupBy     string             `json:"group_by"`
	Aggregation string             `json:"aggregation"`
	ValueField  string             `json:"value_field,omitempty"`
	Rows        []DataAggregateRow `json:"rows"`
}

// SyncTrend is a daily dataset sync trend row.
type SyncTrend struct {
	Date       string `json:"date"`
	Dataset    string `json:"dataset"`
	RowsSynced int64  `json:"rows_synced"`
}

// IdentifierCoverage reports company identifier adoption.
type IdentifierCoverage struct {
	System     string  `json:"system"`
	Count      int     `json:"count"`
	Total      int     `json:"total"`
	Percentage float64 `json:"percentage"`
}

// XrefCoverage reports cross-reference link counts.
type XrefCoverage struct {
	SourceA       string  `json:"source_a"`
	SourceB       string  `json:"source_b"`
	Count         int     `json:"count"`
	AvgConfidence float64 `json:"avg_confidence"`
}

// ScoreDistributionBucket groups enrichment scores into buckets.
type ScoreDistributionBucket struct {
	Bucket int `json:"bucket"`
	Count  int `json:"count"`
}

// PhaseDuration summarizes average phase duration in milliseconds.
type PhaseDuration struct {
	Phase string `json:"phase"`
	AvgMS int64  `json:"avg_ms"`
}

// EnrichmentStats summarizes enrichment run quality and timing.
type EnrichmentStats struct {
	TotalRuns         int                       `json:"total_runs"`
	AvgScore          float64                   `json:"avg_score"`
	ScoreDistribution []ScoreDistributionBucket `json:"score_distribution"`
	PhaseDurations    []PhaseDuration           `json:"phase_durations"`
}

// CostBreakdownRow summarizes cost and token usage by day.
type CostBreakdownRow struct {
	Date   string  `json:"date"`
	Tier   string  `json:"tier"`
	Cost   float64 `json:"cost"`
	Tokens int64   `json:"tokens"`
}
