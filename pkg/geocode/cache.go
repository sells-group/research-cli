package geocode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// cacheKey returns SHA-256 hex of the normalized address for cache lookup.
func cacheKey(addr AddressInput) string {
	normalized := fmt.Sprintf("%s|%s|%s|%s",
		strings.ToLower(strings.TrimSpace(addr.Street)),
		strings.ToLower(strings.TrimSpace(addr.City)),
		strings.ToLower(strings.TrimSpace(addr.State)),
		strings.TrimSpace(addr.ZipCode),
	)
	h := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", h)
}

// checkCache looks up a cached geocode result, respecting TTL if configured.
// Returns cached non-matches (Matched=false) so the caller can skip PostGIS.
func (g *geocoder) checkCache(ctx context.Context, key string) (*Result, error) {
	var lat, lon float64
	var quality string
	var rating *int
	var matched bool
	var countyFIPS *string

	query := "SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache WHERE address_hash = $1"
	args := []any{key}

	if g.cacheTTLDays > 0 {
		query += fmt.Sprintf(" AND cached_at > now() - interval '%d days'", g.cacheTTLDays)
	}

	row := g.pool.QueryRow(ctx, query, args...)
	if err := row.Scan(&lat, &lon, &quality, &rating, &matched, &countyFIPS); err != nil {
		return nil, err // no row or scan error — caller handles
	}

	r := &Result{
		Latitude:  lat,
		Longitude: lon,
		Source:    "tiger",
		Quality:   quality,
		Matched:   matched,
	}
	if rating != nil {
		r.Rating = *rating
	}
	if countyFIPS != nil {
		r.CountyFIPS = *countyFIPS
	}

	keyPrefix := key
	if len(keyPrefix) > 12 {
		keyPrefix = keyPrefix[:12]
	}
	zap.L().Debug("geocode cache hit", zap.String("key", keyPrefix), zap.Bool("matched", matched))
	return r, nil
}

// storeCache inserts a geocode result (match or non-match) into the cache.
func (g *geocoder) storeCache(ctx context.Context, key string, result *Result) error {
	_, err := g.pool.Exec(ctx, `
		INSERT INTO public.geocode_cache (address_hash, latitude, longitude, quality, rating, matched, county_fips, cached_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (address_hash) DO UPDATE SET
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			quality = EXCLUDED.quality,
			rating = EXCLUDED.rating,
			matched = EXCLUDED.matched,
			county_fips = EXCLUDED.county_fips,
			cached_at = now()`,
		key, result.Latitude, result.Longitude, result.Quality, result.Rating, result.Matched, nilIfEmpty(result.CountyFIPS),
	)
	if err != nil {
		return eris.Wrap(err, "geocode: store cache")
	}
	return nil
}

// nilIfEmpty returns nil for empty strings, allowing NULL storage in Postgres.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
