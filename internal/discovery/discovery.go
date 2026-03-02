// Package discovery implements lead discovery via PPP exhaustion and Google Places organic search.
package discovery

import (
	"time"
)

// Candidate represents a potential lead discovered via PPP or organic search.
type Candidate struct {
	ID               int64      `json:"id" db:"id"`
	RunID            string     `json:"run_id" db:"run_id"`
	GooglePlaceID    string     `json:"google_place_id,omitempty" db:"google_place_id"`
	Name             string     `json:"name" db:"name"`
	Domain           string     `json:"domain,omitempty" db:"domain"`
	Website          string     `json:"website,omitempty" db:"website"`
	Street           string     `json:"street,omitempty" db:"street"`
	City             string     `json:"city,omitempty" db:"city"`
	State            string     `json:"state,omitempty" db:"state"`
	ZipCode          string     `json:"zip_code,omitempty" db:"zip_code"`
	NAICSCode        string     `json:"naics_code,omitempty" db:"naics_code"`
	Source           string     `json:"source" db:"source"`
	SourceRecord     []byte     `json:"source_record,omitempty" db:"source_record"`
	Disqualified     bool       `json:"disqualified" db:"disqualified"`
	DisqualifyReason string     `json:"disqualify_reason,omitempty" db:"disqualify_reason"`
	ScoreT0          *float64   `json:"score_t0,omitempty" db:"score_t0"`
	ScoreT1          *float64   `json:"score_t1,omitempty" db:"score_t1"`
	ScoreT2          *float64   `json:"score_t2,omitempty" db:"score_t2"`
	PromotedAt       *time.Time `json:"promoted_at,omitempty" db:"promoted_at"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
}

// RunConfig describes the parameters for a discovery run.
type RunConfig struct {
	Strategy string         `json:"strategy"`
	Params   map[string]any `json:"params"`
}

// RunResult holds the outcome statistics of a discovery run.
type RunResult struct {
	CandidatesFound     int     `json:"candidates_found"`
	CandidatesQualified int     `json:"candidates_qualified"`
	CostUSD             float64 `json:"cost_usd"`
}

// PPPBorrower represents a row from the PPP loan data used for lead discovery.
type PPPBorrower struct {
	BorrowerName string  `json:"borrower_name" db:"borrower_name"`
	Street       string  `json:"street" db:"street"`
	City         string  `json:"city" db:"city"`
	State        string  `json:"state" db:"state"`
	Zip          string  `json:"zip" db:"zip"`
	NAICSCode    string  `json:"naics_code" db:"naics_code"`
	Approval     float64 `json:"approval" db:"current_approval_amount"`
	LoanNumber   string  `json:"loan_number" db:"loan_number"`
}

// ListOpts configures listing and filtering of discovery candidates.
type ListOpts struct {
	Disqualified *bool
	MinScore     *float64
	Limit        int
	Offset       int
}

// PromoteResult summarizes the outcome of promoting candidates to the enrichment pipeline.
type PromoteResult struct {
	Promoted int
	Skipped  int
	Errors   int
}

// GridCell represents a geographic grid cell for organic search.
type GridCell struct {
	ID        int64   `json:"id" db:"id"`
	CBSACode  string  `json:"cbsa_code" db:"cbsa_code"`
	CellKM    float64 `json:"cell_km" db:"cell_km"`
	SWLat     float64 `json:"sw_lat" db:"sw_lat"`
	SWLon     float64 `json:"sw_lon" db:"sw_lon"`
	NELat     float64 `json:"ne_lat" db:"ne_lat"`
	NELon     float64 `json:"ne_lon" db:"ne_lon"`
	ResultCnt *int    `json:"result_count,omitempty" db:"result_count"`
}
