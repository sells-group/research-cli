package scraper

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

const geocodePPPSQL = `
UPDATE geo.infrastructure
SET properties = properties || jsonb_build_object(
    'geocode_pending', true
)
WHERE source = 'ppp' AND latitude = 0 AND longitude = 0
AND NOT COALESCE((properties->>'geocode_pending')::boolean, false)
`

// GeocodePPP enqueues PPP loan addresses for geocoding by flagging them.
type GeocodePPP struct{}

// Name implements GeoScraper.
func (s *GeocodePPP) Name() string { return "geocode_ppp" }

// Table implements GeoScraper.
func (s *GeocodePPP) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *GeocodePPP) Category() geoscraper.Category { return geoscraper.OnDemand }

// Cadence implements GeoScraper.
func (s *GeocodePPP) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper. Geocoding is manual only.
func (s *GeocodePPP) ShouldRun(_ time.Time, _ *time.Time) bool { return false }

// Sync implements GeoScraper.
func (s *GeocodePPP) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting geocode_ppp")

	tag, err := pool.Exec(ctx, geocodePPPSQL)
	if err != nil {
		return nil, eris.Wrap(err, "geocode_ppp: exec")
	}

	rows := tag.RowsAffected()
	log.Info("geocode_ppp complete", zap.Int64("rows", rows))
	return &geoscraper.SyncResult{RowsSynced: rows}, nil
}
