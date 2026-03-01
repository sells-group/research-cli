package geoscraper

import (
	"context"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// Category groups geo scrapers by scope.
type Category int

const (
	// National covers nationwide sources (HIFLD, FEMA, EPA, Census).
	National Category = iota + 1
	// State covers per-state sources (SoS, assessor, permits).
	State
	// OnDemand covers scrapers triggered manually.
	OnDemand
)

// String returns the human-readable category name.
func (c Category) String() string {
	switch c {
	case National:
		return "national"
	case State:
		return "state"
	case OnDemand:
		return "on_demand"
	default:
		return "unknown"
	}
}

// ParseCategory converts a string into a Category.
func ParseCategory(s string) (Category, error) {
	switch s {
	case "national":
		return National, nil
	case "state":
		return State, nil
	case "on_demand", "on-demand":
		return OnDemand, nil
	default:
		return 0, eris.Errorf("unknown category: %q (valid: national, state, on_demand)", s)
	}
}

// SyncResult holds the outcome of a scraper sync. Alias of the fedsync type
// so scraper implementations don't need to import the fedsync package.
type SyncResult = dataset.SyncResult

// Cadence re-exports the fedsync cadence type for scraper convenience.
type Cadence = dataset.Cadence

// Re-export cadence constants so scrapers can reference them directly.
const (
	Daily     = dataset.Daily
	Weekly    = dataset.Weekly
	Monthly   = dataset.Monthly
	Quarterly = dataset.Quarterly
	Annual    = dataset.Annual
)

// GeoScraper defines the interface each geo data source must implement.
type GeoScraper interface {
	// Name returns the unique identifier (e.g., "hifld", "fema_flood", "epa_echo").
	Name() string

	// Table returns the target table (e.g., "geo.infrastructure", "geo.poi").
	Table() string

	// Category returns the scraper scope: National, State, or OnDemand.
	Category() Category

	// Cadence returns how often the upstream source is updated.
	Cadence() Cadence

	// ShouldRun decides if this scraper needs running given the current time
	// and the time of the last successful sync (nil if never synced).
	ShouldRun(now time.Time, lastSync *time.Time) bool

	// Sync performs the data download, parse, and load into Postgres.
	Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error)
}

// StateScraper is an optional interface for scrapers scoped to specific states.
type StateScraper interface {
	GeoScraper
	// States returns the FIPS codes this scraper covers (e.g., ["48", "12", "06"]).
	States() []string
}

// AddressProducer is an optional interface scrapers implement to indicate they
// produce rows with addresses that should be enqueued for geocoding after sync.
type AddressProducer interface {
	// HasAddresses returns true if the scraper's target table contains an address column.
	HasAddresses() bool
}
