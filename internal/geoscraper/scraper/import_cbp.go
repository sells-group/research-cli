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

const importCBPSQL = `
INSERT INTO geo.cbp_summary (fips, state_fips, county_name, year, establishments, employees, payroll, source, source_id, properties)
SELECT
    fips_state || fips_county,
    fips_state,
    '',
    year,
    COALESCE(est, 0),
    COALESCE(emp, 0),
    COALESCE(ap, 0)::bigint * 1000,
    'cbp',
    fips_state || fips_county || '_' || year::text,
    '{}'::jsonb
FROM fed_data.cbp_data
WHERE fips_county IS NOT NULL AND fips_county != '' AND naics = '000000'
ON CONFLICT (fips, year) DO UPDATE SET
    establishments = EXCLUDED.establishments,
    employees = EXCLUDED.employees,
    payroll = EXCLUDED.payroll,
    updated_at = now()
`

// ImportCBP imports Census CBP county totals from fed_data into geo.cbp_summary.
type ImportCBP struct{}

// Name implements GeoScraper.
func (s *ImportCBP) Name() string { return "import_cbp" }

// Table implements GeoScraper.
func (s *ImportCBP) Table() string { return "geo.cbp_summary" }

// Category implements GeoScraper.
func (s *ImportCBP) Category() geoscraper.Category { return geoscraper.OnDemand }

// Cadence implements GeoScraper.
func (s *ImportCBP) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper. Cross-DB imports are manual only.
func (s *ImportCBP) ShouldRun(_ time.Time, _ *time.Time) bool { return false }

// Sync implements GeoScraper.
func (s *ImportCBP) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting import_cbp")

	tag, err := pool.Exec(ctx, importCBPSQL)
	if err != nil {
		return nil, eris.Wrap(err, "import_cbp: exec")
	}

	rows := tag.RowsAffected()
	log.Info("import_cbp complete", zap.Int64("rows", rows))
	return &geoscraper.SyncResult{RowsSynced: rows}, nil
}
