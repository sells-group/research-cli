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

const importQCEWSQL = `
INSERT INTO geo.qcew_summary (fips, state_fips, county_name, year, quarter, avg_weekly_wage, total_wages, month3_employment, establishments, source, source_id, properties)
SELECT
    area_fips,
    LEFT(area_fips, 2),
    '',
    year,
    qtr,
    COALESCE(avg_wkly_wage, 0),
    COALESCE(total_qtrly_wages, 0),
    COALESCE(month3_emplvl, 0),
    COALESCE(qtrly_estabs, 0),
    'qcew',
    area_fips || '_' || year::text || '_Q' || qtr::text,
    '{}'::jsonb
FROM fed_data.qcew_data
WHERE area_fips IS NOT NULL AND area_fips != '' AND own_code = '5' AND industry_code = '10'
ON CONFLICT (fips, year, quarter) DO UPDATE SET
    avg_weekly_wage = EXCLUDED.avg_weekly_wage,
    total_wages = EXCLUDED.total_wages,
    month3_employment = EXCLUDED.month3_employment,
    establishments = EXCLUDED.establishments,
    updated_at = now()
`

// ImportQCEW imports BLS QCEW county summaries from fed_data into geo.qcew_summary.
type ImportQCEW struct{}

// Name implements GeoScraper.
func (s *ImportQCEW) Name() string { return "import_qcew" }

// Table implements GeoScraper.
func (s *ImportQCEW) Table() string { return "geo.qcew_summary" }

// Category implements GeoScraper.
func (s *ImportQCEW) Category() geoscraper.Category { return geoscraper.OnDemand }

// Cadence implements GeoScraper.
func (s *ImportQCEW) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper. Cross-DB imports are manual only.
func (s *ImportQCEW) ShouldRun(_ time.Time, _ *time.Time) bool { return false }

// Sync implements GeoScraper.
func (s *ImportQCEW) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting import_qcew")

	tag, err := pool.Exec(ctx, importQCEWSQL)
	if err != nil {
		return nil, eris.Wrap(err, "import_qcew: exec")
	}

	rows := tag.RowsAffected()
	log.Info("import_qcew complete", zap.Int64("rows", rows))
	return &geoscraper.SyncResult{RowsSynced: rows}, nil
}
