package schedules

import (
	"context"
	"fmt"
	"time"

	"github.com/sells-group/research-cli/internal/fetcher"
)

// HeadCheck does a HEAD request and compares ETag against the value stored
// in sync_log metadata from the previous successful sync.
func HeadCheck(url string) AvailabilityCheckFn {
	return func(ctx context.Context, f fetcher.Fetcher, _ *time.Time, lastMeta map[string]any) (bool, map[string]any, error) {
		etag, err := f.HeadETag(ctx, url)
		if err != nil {
			return false, nil, err
		}

		meta := map[string]any{"etag": etag, "url": url}

		// If we have a previous ETag, compare.
		if lastMeta != nil {
			if prevETag, ok := lastMeta["etag"].(string); ok && prevETag == etag && etag != "" {
				return false, meta, nil
			}
		}

		return true, meta, nil
	}
}

// URLProbe tries to HEAD a date-templated URL and returns true if 200.
// argsFn generates the format arguments from the current time.
func URLProbe(pattern string, argsFn func(time.Time) []any) AvailabilityCheckFn {
	return func(ctx context.Context, f fetcher.Fetcher, _ *time.Time, _ map[string]any) (bool, map[string]any, error) {
		args := argsFn(time.Now().UTC())
		url := fmt.Sprintf(pattern, args...)

		etag, err := f.HeadETag(ctx, url)
		if err != nil {
			// URL doesn't exist or errored → not available.
			return false, nil, nil //nolint:nilerr
		}

		meta := map[string]any{"url": url}
		if etag != "" {
			meta["etag"] = etag
		}
		return true, meta, nil
	}
}

// CompositeCheck runs multiple checks and returns true if any passes.
func CompositeCheck(checks ...AvailabilityCheckFn) AvailabilityCheckFn {
	return func(ctx context.Context, f fetcher.Fetcher, lastSync *time.Time, lastMeta map[string]any) (bool, map[string]any, error) {
		for _, check := range checks {
			available, meta, err := check(ctx, f, lastSync, lastMeta)
			if err != nil {
				continue
			}
			if available {
				return true, meta, nil
			}
		}
		return false, nil, nil
	}
}
