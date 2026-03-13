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

const importEPASQL = `
INSERT INTO geo.epa_sites (name, program, registry_id, facility_name, state, city, zip, latitude, longitude, source, source_id, properties)
SELECT
    COALESCE(fac_name, ''),
    'epa_echo',
    registry_id,
    fac_name,
    fac_state,
    fac_city,
    fac_zip,
    COALESCE(fac_lat, 0),
    COALESCE(fac_long, 0),
    'epa',
    registry_id,
    jsonb_build_object(
        'sic_codes', sic_codes,
        'naics_codes', naics_codes
    )
FROM fed_data.epa_facilities
WHERE registry_id IS NOT NULL AND registry_id != ''
ON CONFLICT (registry_id) DO UPDATE SET
    name = EXCLUDED.name,
    facility_name = EXCLUDED.facility_name,
    state = EXCLUDED.state,
    city = EXCLUDED.city,
    zip = EXCLUDED.zip,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    source = EXCLUDED.source,
    source_id = EXCLUDED.source_id,
    properties = EXCLUDED.properties,
    updated_at = now()
`

// ImportEPA imports EPA ECHO facilities from fed_data into geo.epa_sites.
type ImportEPA struct{}

// Name implements GeoScraper.
func (s *ImportEPA) Name() string { return "import_epa" }

// Table implements GeoScraper.
func (s *ImportEPA) Table() string { return "geo.epa_sites" }

// Category implements GeoScraper.
func (s *ImportEPA) Category() geoscraper.Category { return geoscraper.OnDemand }

// Cadence implements GeoScraper.
func (s *ImportEPA) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper. Cross-DB imports are manual only.
func (s *ImportEPA) ShouldRun(_ time.Time, _ *time.Time) bool { return false }

// Sync implements GeoScraper.
func (s *ImportEPA) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting import_epa")

	tag, err := pool.Exec(ctx, importEPASQL)
	if err != nil {
		return nil, eris.Wrap(err, "import_epa: exec")
	}

	rows := tag.RowsAffected()
	log.Info("import_epa complete", zap.Int64("rows", rows))
	return &geoscraper.SyncResult{RowsSynced: rows}, nil
}
