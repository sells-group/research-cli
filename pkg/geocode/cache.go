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

// checkCache looks up a cached geocode result.
func (g *geocoder) checkCache(ctx context.Context, key string) (*Result, error) {
	var lat, lon float64
	var quality string
	var rating *int

	row := g.pool.QueryRow(ctx,
		"SELECT latitude, longitude, quality, rating FROM public.geocode_cache WHERE address_hash = $1",
		key,
	)
	if err := row.Scan(&lat, &lon, &quality, &rating); err != nil {
		return nil, err // no row or scan error — caller handles
	}

	r := &Result{
		Latitude:  lat,
		Longitude: lon,
		Source:    "tiger",
		Quality:   quality,
		Matched:   true,
	}
	if rating != nil {
		r.Rating = *rating
	}

	keyPrefix := key
	if len(keyPrefix) > 12 {
		keyPrefix = keyPrefix[:12]
	}
	zap.L().Debug("geocode cache hit", zap.String("key", keyPrefix))
	return r, nil
}

// storeCache inserts a geocode result into the cache.
func (g *geocoder) storeCache(ctx context.Context, key string, result *Result) error {
	_, err := g.pool.Exec(ctx, `
		INSERT INTO public.geocode_cache (address_hash, latitude, longitude, quality, rating, cached_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (address_hash) DO UPDATE SET
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			quality = EXCLUDED.quality,
			rating = EXCLUDED.rating,
			cached_at = now()`,
		key, result.Latitude, result.Longitude, result.Quality, result.Rating,
	)
	if err != nil {
		return eris.Wrap(err, "geocode: store cache")
	}
	return nil
}
