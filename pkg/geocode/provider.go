package geocode

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
)

// Provider represents a single geocoding backend.
type Provider interface {
	Name() string
	Geocode(ctx context.Context, addr AddressInput) (*Result, error)
	Available() bool
}

// TigerProvider geocodes via PostGIS TIGER/Line data.
type TigerProvider struct {
	pool      db.Pool
	maxRating int
}

// NewTigerProvider creates a TigerProvider with the given pool and max rating threshold.
func NewTigerProvider(pool db.Pool, maxRating int) *TigerProvider {
	return &TigerProvider{pool: pool, maxRating: maxRating}
}

// Name implements Provider.
func (p *TigerProvider) Name() string { return "tiger" }

// Available implements Provider.
func (p *TigerProvider) Available() bool { return true }

// Geocode implements Provider.
func (p *TigerProvider) Geocode(ctx context.Context, addr AddressInput) (*Result, error) {
	oneLine := formatOneLine(addr)
	if oneLine == "" {
		return &Result{Matched: false, Source: "tiger"}, nil
	}

	var lat, lon float64
	var rating int
	var matchedAddr string
	var countyFIPS sql.NullString

	row := p.pool.QueryRow(ctx, `
		SELECT
			ST_Y(geomout) AS lat,
			ST_X(geomout) AS lon,
			rating,
			pprint_addy(addy) AS matched_address,
			(addy).statefp || (addy).countyfp AS county_fips
		FROM geocode($1, 1)`,
		oneLine,
	)

	err := row.Scan(&lat, &lon, &rating, &matchedAddr, &countyFIPS)
	if err != nil {
		zap.L().Debug("tiger provider: no match",
			zap.String("address", oneLine),
			zap.Error(err),
		)
		return &Result{Matched: false, Source: "tiger"}, nil
	}

	if rating > p.maxRating {
		zap.L().Debug("tiger provider: rating exceeds threshold",
			zap.String("address", oneLine),
			zap.Int("rating", rating),
			zap.Int("max_rating", p.maxRating),
		)
		return &Result{Matched: false, Source: "tiger", Rating: rating}, nil
	}

	result := &Result{
		Latitude:  lat,
		Longitude: lon,
		Source:    "tiger",
		Quality:   ratingToQuality(rating),
		Matched:   true,
		Rating:    rating,
	}
	if countyFIPS.Valid {
		result.CountyFIPS = countyFIPS.String
	}
	return result, nil
}

// CascadeClient tries geocode providers in order until one succeeds.
type CascadeClient struct {
	providers        []Provider
	pool             db.Pool
	cacheEnabled     bool
	cacheTTLDays     int
	cacheTable       string
	batchConcurrency int
}

// CascadeOption configures the CascadeClient.
type CascadeOption func(*CascadeClient)

// WithCascadeCacheEnabled enables or disables caching on the cascade client.
func WithCascadeCacheEnabled(enabled bool) CascadeOption {
	return func(c *CascadeClient) {
		c.cacheEnabled = enabled
	}
}

// WithCascadeCacheTTLDays sets the cache TTL in days for the cascade client.
func WithCascadeCacheTTLDays(days int) CascadeOption {
	return func(c *CascadeClient) {
		c.cacheTTLDays = days
	}
}

// WithCascadeCacheTable sets the cache table name for the cascade client.
func WithCascadeCacheTable(table string) CascadeOption {
	return func(c *CascadeClient) {
		c.cacheTable = table
	}
}

// WithCascadeBatchConcurrency sets the max parallel calls for BatchGeocode.
func WithCascadeBatchConcurrency(n int) CascadeOption {
	return func(c *CascadeClient) {
		if n > 0 {
			c.batchConcurrency = n
		}
	}
}

// NewCascadeClient creates a CascadeClient that tries providers in order.
func NewCascadeClient(pool db.Pool, providers []Provider, opts ...CascadeOption) *CascadeClient {
	c := &CascadeClient{
		providers:        providers,
		pool:             pool,
		cacheEnabled:     true,
		cacheTable:       "public.geocode_cache",
		batchConcurrency: 10,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Geocode implements Client by trying each provider in order.
func (c *CascadeClient) Geocode(ctx context.Context, addr AddressInput) (*Result, error) {
	key := cacheKey(addr)

	if c.cacheEnabled {
		cached, err := c.checkCache(ctx, key)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	var lastResult *Result
	for _, p := range c.providers {
		if !p.Available() {
			continue
		}
		result, err := p.Geocode(ctx, addr)
		if err != nil {
			zap.L().Debug("cascade: provider error, trying next",
				zap.String("provider", p.Name()),
				zap.Error(err),
			)
			continue
		}
		if result != nil && result.Matched {
			if c.cacheEnabled {
				_ = c.storeCache(ctx, key, result)
			}
			return result, nil
		}
		if result != nil {
			lastResult = result
		}
	}

	// All providers missed — cache negative result and return unmatched.
	noMatch := &Result{Matched: false, Source: "cascade"}
	if lastResult != nil {
		noMatch.Source = lastResult.Source
		noMatch.Rating = lastResult.Rating
	}
	if c.cacheEnabled {
		_ = c.storeCache(ctx, key, noMatch)
	}
	return noMatch, nil
}

// BatchGeocode implements Client by geocoding addresses in parallel.
func (c *CascadeClient) BatchGeocode(ctx context.Context, addrs []AddressInput) ([]Result, error) {
	if len(addrs) == 0 {
		return nil, nil
	}

	for i := range addrs {
		if addrs[i].ID == "" {
			addrs[i].ID = fmt.Sprintf("%d", i)
		}
	}

	results := make([]Result, len(addrs))

	eg, gCtx := errgroup.WithContext(ctx)
	eg.SetLimit(c.batchConcurrency)

	for i, addr := range addrs {
		eg.Go(func() error {
			r, gcErr := c.Geocode(gCtx, addr)
			if gcErr != nil || r == nil {
				results[i] = Result{Matched: false, Source: "cascade"}
				return nil //nolint:nilerr // individual geocode failures don't fail the batch
			}
			results[i] = *r
			return nil
		})
	}

	_ = eg.Wait()
	return results, nil
}

// ReverseGeocode implements Client by delegating to the package-level function.
func (c *CascadeClient) ReverseGeocode(ctx context.Context, lat, lng float64) (*ReverseResult, error) {
	return ReverseGeocode(ctx, c.pool, lat, lng)
}

// checkCache looks up a cached geocode result for the cascade client.
func (c *CascadeClient) checkCache(ctx context.Context, key string) (*Result, error) {
	var lat, lon float64
	var quality string
	var rating *int
	var matched bool
	var countyFIPS *string
	var source *string

	query := fmt.Sprintf("SELECT latitude, longitude, quality, rating, matched, county_fips, source FROM %s WHERE address_hash = $1", c.cacheTable)
	args := []any{key}

	if c.cacheTTLDays > 0 {
		query += fmt.Sprintf(" AND cached_at > now() - interval '%d days'", c.cacheTTLDays)
	}

	row := c.pool.QueryRow(ctx, query, args...)
	if err := row.Scan(&lat, &lon, &quality, &rating, &matched, &countyFIPS, &source); err != nil {
		return nil, err
	}

	r := &Result{
		Latitude:  lat,
		Longitude: lon,
		Source:    "cascade",
		Quality:   quality,
		Matched:   matched,
	}
	if rating != nil {
		r.Rating = *rating
	}
	if countyFIPS != nil {
		r.CountyFIPS = *countyFIPS
	}
	if source != nil {
		r.Source = *source
	}

	keyPrefix := key
	if len(keyPrefix) > 12 {
		keyPrefix = keyPrefix[:12]
	}
	zap.L().Debug("cascade cache hit", zap.String("key", keyPrefix), zap.Bool("matched", matched))
	return r, nil
}

// storeCache inserts a geocode result into the cascade cache.
func (c *CascadeClient) storeCache(ctx context.Context, key string, result *Result) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (address_hash, latitude, longitude, quality, rating, matched, county_fips, source, cached_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (address_hash) DO UPDATE SET
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			quality = EXCLUDED.quality,
			rating = EXCLUDED.rating,
			matched = EXCLUDED.matched,
			county_fips = EXCLUDED.county_fips,
			source = EXCLUDED.source,
			cached_at = now()`, c.cacheTable)

	_, err := c.pool.Exec(ctx, query,
		key, result.Latitude, result.Longitude, result.Quality, result.Rating, result.Matched, nilIfEmpty(result.CountyFIPS), result.Source,
	)
	if err != nil {
		return eris.Wrap(err, "cascade: store cache")
	}
	return nil
}
