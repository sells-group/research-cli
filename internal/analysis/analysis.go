package analysis

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// Category groups analyzers by analysis type.
type Category int

const (
	// Spatial covers proximity matrix and distance calculations.
	Spatial Category = iota + 1
	// Scoring covers composite parcel/property scoring.
	Scoring
	// Correlation covers cross-source data correlation and owner analysis.
	Correlation
	// Ranking covers opportunity ranking and prioritization.
	Ranking
	// Export covers data export and report generation.
	Export
)

// String returns the human-readable category name.
func (c Category) String() string {
	switch c {
	case Spatial:
		return "spatial"
	case Scoring:
		return "scoring"
	case Correlation:
		return "correlation"
	case Ranking:
		return "ranking"
	case Export:
		return "export"
	default:
		return "unknown"
	}
}

// ParseCategory converts a string into a Category.
func ParseCategory(s string) (Category, error) {
	switch s {
	case "spatial":
		return Spatial, nil
	case "scoring":
		return Scoring, nil
	case "correlation":
		return Correlation, nil
	case "ranking":
		return Ranking, nil
	case "export":
		return Export, nil
	default:
		return 0, eris.Errorf("unknown analysis category: %q (valid: spatial, scoring, correlation, ranking, export)", s)
	}
}

// RunResult holds the outcome of an analyzer run.
type RunResult struct {
	RowsAffected int64          `json:"rows_affected"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// RunOpts configures which analyzers to run and how.
type RunOpts struct {
	Category  *Category // restrict to a specific category
	Analyzers []string  // restrict to specific analyzer names
	Force     bool      // ignore validation checks
}

// Analyzer defines the interface each analysis step must implement.
type Analyzer interface {
	// Name returns the unique identifier (e.g., "proximity_matrix", "parcel_scores").
	Name() string

	// Category returns the analysis category.
	Category() Category

	// Dependencies returns names of analyzers that must complete before this one.
	// Return nil or empty for no dependencies.
	Dependencies() []string

	// Validate checks that required source data exists. Returns nil if ready.
	Validate(ctx context.Context, pool db.Pool) error

	// Run performs the analysis and writes results to the database.
	Run(ctx context.Context, pool db.Pool, opts RunOpts) (*RunResult, error)
}
