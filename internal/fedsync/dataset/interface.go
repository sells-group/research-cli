package dataset

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/fetcher"
)

// Phase represents a pipeline phase for grouping datasets.
type Phase int

const (
	Phase1  Phase = iota + 1 // Market Intelligence (Census, BLS, SAM)
	Phase1B                  // Buyer Intelligence (SEC/EDGAR)
	Phase2                   // Extended Intelligence (FINRA, OSHA, EPA, Census)
	Phase3                   // On-Demand (XBRL, FRED, CRS)
)

// String returns the human-readable phase name.
func (p Phase) String() string {
	switch p {
	case Phase1:
		return "1"
	case Phase1B:
		return "1b"
	case Phase2:
		return "2"
	case Phase3:
		return "3"
	default:
		return "unknown"
	}
}

// ParsePhase converts a string like "1", "1b", "2", "3" into a Phase.
func ParsePhase(s string) (Phase, error) {
	switch s {
	case "1":
		return Phase1, nil
	case "1b", "1B":
		return Phase1B, nil
	case "2":
		return Phase2, nil
	case "3":
		return Phase3, nil
	default:
		return 0, eris.Errorf("unknown phase: %q (valid: 1, 1b, 2, 3)", s)
	}
}

// Cadence describes how often a dataset should be synced.
type Cadence string

const (
	Daily     Cadence = "daily"
	Weekly    Cadence = "weekly"
	Monthly   Cadence = "monthly"
	Quarterly Cadence = "quarterly"
	Annual    Cadence = "annual"
)

// SyncResult holds the outcome of a dataset sync.
type SyncResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// Dataset defines the interface each federal dataset must implement.
type Dataset interface {
	// Name returns the unique identifier for this dataset (e.g., "cbp", "adv_part1").
	Name() string

	// Table returns the primary target table (e.g., "fed_data.cbp_data").
	Table() string

	// Phase returns which pipeline phase this dataset belongs to.
	Phase() Phase

	// Cadence returns how often this dataset is updated upstream.
	Cadence() Cadence

	// ShouldRun decides if this dataset needs syncing given the current time
	// and the time of the last successful sync (nil if never synced).
	ShouldRun(now time.Time, lastSync *time.Time) bool

	// Sync performs the actual data download, parse, and load into Postgres.
	// tempDir is a working directory for temporary files.
	Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error)
}
