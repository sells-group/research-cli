// Package geocode provides address geocoding via PostGIS tiger geocoder.
package geocode

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
)

// Client geocodes addresses using PostGIS tiger geocoder.
type Client interface {
	// Geocode geocodes a single address.
	Geocode(ctx context.Context, addr AddressInput) (*Result, error)

	// BatchGeocode geocodes multiple addresses.
	BatchGeocode(ctx context.Context, addrs []AddressInput) ([]Result, error)

	// ReverseGeocode converts lat/lng to a street address.
	ReverseGeocode(ctx context.Context, lat, lng float64) (*ReverseResult, error)
}

// AddressInput represents an address to geocode.
type AddressInput struct {
	ID      string // Optional identifier for batch correlation
	Street  string
	City    string
	State   string
	ZipCode string
}

// Result holds the geocoding output for an address.
type Result struct {
	Latitude   float64
	Longitude  float64
	Source     string // "tiger"
	Quality    string // "rooftop", "range", "centroid", "approximate"
	Matched    bool
	Rating     int    // PostGIS geocoder rating (0=best)
	CountyFIPS string // 5-digit state+county FIPS (e.g., "48453" for Travis County TX)
}

// Option configures the geocoder.
type Option func(*geocoder)

// WithCacheEnabled enables or disables the geocode result cache.
func WithCacheEnabled(enabled bool) Option {
	return func(g *geocoder) {
		g.cacheEnabled = enabled
	}
}

// WithMaxRating sets the maximum acceptable geocoder rating.
// Results with ratings above this threshold are treated as unmatched.
// Default is 100.
func WithMaxRating(maxRating int) Option {
	return func(g *geocoder) {
		g.maxRating = maxRating
	}
}

// WithCacheTTLDays sets the cache time-to-live in days.
// Cached entries older than this are ignored. 0 means no expiry.
func WithCacheTTLDays(days int) Option {
	return func(g *geocoder) {
		g.cacheTTLDays = days
	}
}

// WithBatchConcurrency sets the maximum parallel PostGIS calls for BatchGeocode.
// Default is 10.
func WithBatchConcurrency(n int) Option {
	return func(g *geocoder) {
		if n > 0 {
			g.batchConcurrency = n
		}
	}
}

// WithCacheTable sets the cache table name. Default is "public.geocode_cache".
func WithCacheTable(table string) Option {
	return func(g *geocoder) {
		g.cacheTable = table
	}
}

type geocoder struct {
	pool             db.Pool
	cacheEnabled     bool
	maxRating        int
	cacheTTLDays     int
	batchConcurrency int
	cacheTable       string
}

// NewClient creates a new geocoding Client backed by PostGIS tiger geocoder.
func NewClient(pool db.Pool, opts ...Option) Client {
	g := &geocoder{
		pool:             pool,
		cacheEnabled:     true,
		maxRating:        100,
		batchConcurrency: 10,
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Geocode geocodes a single address using PostGIS tiger geocoder.
func (g *geocoder) Geocode(ctx context.Context, addr AddressInput) (*Result, error) {
	key := cacheKey(addr)

	// Check cache first.
	if g.cacheEnabled {
		cached, err := g.checkCache(ctx, key)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	// Call PostGIS geocode().
	result, err := g.tigerGeocode(ctx, addr)
	if err != nil {
		return nil, err
	}

	// Store in cache (both matches and non-matches for negative caching).
	if g.cacheEnabled {
		_ = g.storeCache(ctx, key, result)
	}

	return result, nil
}

// BatchGeocode geocodes multiple addresses using parallel PostGIS calls.
func (g *geocoder) BatchGeocode(ctx context.Context, addrs []AddressInput) ([]Result, error) {
	if len(addrs) == 0 {
		return nil, nil
	}

	// Assign IDs for batch correlation if not set.
	for i := range addrs {
		if addrs[i].ID == "" {
			addrs[i].ID = fmt.Sprintf("%d", i)
		}
	}

	results := make([]Result, len(addrs))

	eg, gCtx := errgroup.WithContext(ctx)
	eg.SetLimit(g.batchConcurrency)

	for i, addr := range addrs {
		eg.Go(func() error {
			r, gcErr := g.Geocode(gCtx, addr)
			if gcErr != nil || r == nil {
				results[i] = Result{Matched: false, Source: "tiger"}
				return nil //nolint:nilerr // individual geocode failures don't fail the batch
			}
			results[i] = *r
			return nil
		})
	}

	_ = eg.Wait() // errors are swallowed per-address above
	return results, nil
}

// ReverseGeocode implements Client by delegating to the package-level function.
func (g *geocoder) ReverseGeocode(ctx context.Context, lat, lng float64) (*ReverseResult, error) {
	return ReverseGeocode(ctx, g.pool, lat, lng)
}

// formatOneLine formats an address as a single line for the geocoder.
func formatOneLine(addr AddressInput) string {
	parts := []string{addr.Street, addr.City, addr.State, addr.ZipCode}
	var nonEmpty []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, ", ")
}
