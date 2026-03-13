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

const importPPPSQL = `
INSERT INTO geo.infrastructure (name, type, fuel_type, capacity, latitude, longitude, source, source_id, properties)
SELECT
    COALESCE(borrowername, ''),
    'ppp_loan',
    naicscode,
    currentapprovalamount,
    0, 0,
    'ppp',
    loannumber::text,
    jsonb_build_object(
        'jobs_reported', jobsreported,
        'city', borrowercity,
        'state', borrowerstate,
        'zip', borrowerzip,
        'forgiveness', forgivenessamount
    )
FROM fed_data.ppp_loans
WHERE loannumber IS NOT NULL
ON CONFLICT (source, source_id) DO UPDATE SET
    name = EXCLUDED.name,
    capacity = EXCLUDED.capacity,
    properties = EXCLUDED.properties,
    updated_at = now()
`

// ImportPPP imports PPP loan data from fed_data into geo.infrastructure.
type ImportPPP struct{}

// Name implements GeoScraper.
func (s *ImportPPP) Name() string { return "import_ppp" }

// Table implements GeoScraper.
func (s *ImportPPP) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *ImportPPP) Category() geoscraper.Category { return geoscraper.OnDemand }

// Cadence implements GeoScraper.
func (s *ImportPPP) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper. Cross-DB imports are manual only.
func (s *ImportPPP) ShouldRun(_ time.Time, _ *time.Time) bool { return false }

// Sync implements GeoScraper.
func (s *ImportPPP) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting import_ppp")

	tag, err := pool.Exec(ctx, importPPPSQL)
	if err != nil {
		return nil, eris.Wrap(err, "import_ppp: exec")
	}

	rows := tag.RowsAffected()
	log.Info("import_ppp complete", zap.Int64("rows", rows))
	return &geoscraper.SyncResult{RowsSynced: rows}, nil
}
