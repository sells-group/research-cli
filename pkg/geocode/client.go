// Package geocode provides address geocoding via Census Geocoder (primary) and Google (fallback).
package geocode

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

// Client geocodes addresses using Census Geocoder (primary) and Google (fallback).
type Client interface {
	// Geocode geocodes a single address.
	Geocode(ctx context.Context, addr AddressInput) (*Result, error)

	// BatchGeocode geocodes multiple addresses.
	BatchGeocode(ctx context.Context, addrs []AddressInput) ([]Result, error)
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
	Latitude  float64
	Longitude float64
	Source    string // "census" or "google"
	Quality   string // "rooftop", "range", "centroid", "approximate"
	Matched   bool
}

// Option configures the geocoder.
type Option func(*geocoder)

// WithGoogleAPIKey enables Google Geocoding API as a fallback.
func WithGoogleAPIKey(key string) Option {
	return func(g *geocoder) {
		g.googleKey = key
	}
}

// WithHTTPClient sets a custom HTTP client for both Census and Google requests.
func WithHTTPClient(hc *http.Client) Option {
	return func(g *geocoder) {
		g.httpClient = hc
	}
}

// WithRateLimit sets the requests-per-second rate limit for Census API calls.
func WithRateLimit(rps float64) Option {
	return func(g *geocoder) {
		g.limiter = rate.NewLimiter(rate.Limit(rps), int(rps))
	}
}

type geocoder struct {
	httpClient *http.Client
	googleKey  string
	limiter    *rate.Limiter
}

// NewClient creates a new geocoding Client with the given options.
func NewClient(opts ...Option) Client {
	g := &geocoder{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		limiter:    rate.NewLimiter(50, 50), // Census default: 50 req/s
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Geocode geocodes a single address, trying Census first, then Google if configured.
func (g *geocoder) Geocode(ctx context.Context, addr AddressInput) (*Result, error) {
	result, censusErr := g.geocodeCensus(ctx, addr)
	if censusErr == nil && result.Matched {
		return result, nil
	}

	// If Census failed or didn't match, try Google if configured.
	if g.googleKey != "" {
		googleResult, googleErr := g.geocodeGoogle(ctx, addr)
		if googleErr == nil && googleResult.Matched {
			return googleResult, nil
		}
	}

	// No match from any provider — this is not an error, just unmatched.
	return &Result{Matched: false}, nil
}

// BatchGeocode geocodes multiple addresses using Census batch API, falling back
// to Google for individual unmatched addresses.
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

	// Try Census batch geocoding.
	results, err := g.batchGeocodeCensus(ctx, addrs)
	if err != nil {
		// Fall back to individual geocoding.
		results = make([]Result, len(addrs))
		for i, addr := range addrs {
			r, geocodeErr := g.Geocode(ctx, addr)
			if geocodeErr != nil {
				results[i] = Result{Matched: false}
				continue
			}
			results[i] = *r
		}
		return results, nil
	}

	// For unmatched Census results, try Google individually if configured.
	if g.googleKey != "" {
		for i, r := range results {
			if !r.Matched {
				googleResult, googleErr := g.geocodeGoogle(ctx, addrs[i])
				if googleErr == nil && googleResult.Matched {
					results[i] = *googleResult
				}
			}
		}
	}

	return results, nil
}
