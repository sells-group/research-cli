package schedules

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/fetcher"
)

// AvailabilityCheckFn checks if new data exists upstream.
// Returns (available, metadata, error).
type AvailabilityCheckFn func(ctx context.Context, f fetcher.Fetcher, lastSync *time.Time, lastMeta map[string]any) (bool, map[string]any, error)

// AvailabilityChecker is implemented by datasets/scrapers that can cheaply detect new data.
type AvailabilityChecker interface {
	// CheckAvailable returns true if new data exists upstream.
	// Should be cheap: HEAD request, ETag check, manifest probe, or URL regex.
	CheckAvailable(ctx context.Context, f fetcher.Fetcher, lastSync *time.Time, lastMeta map[string]any) (bool, map[string]any, error)
}

// MetadataConsumer allows a dataset to receive metadata from the availability check.
type MetadataConsumer interface {
	SetSyncMetadata(meta map[string]any)
}
