package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// CPSLAUS syncs BLS CPS/LAUS local area unemployment data.
type CPSLAUS struct {
	cfg *config.Config
}

func (d *CPSLAUS) Name() string     { return "cps_laus" }
func (d *CPSLAUS) Table() string    { return "fed_data.laus_data" }
func (d *CPSLAUS) Phase() Phase     { return Phase3 }
func (d *CPSLAUS) Cadence() Cadence { return Monthly }

func (d *CPSLAUS) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// LAUS target series: state-level unemployment rates for top financial states.
var lausSeries = []string{
	"LASST060000000000003", // California unemployment rate
	"LASST360000000000003", // New York
	"LASST120000000000003", // Florida
	"LASST480000000000003", // Texas
	"LASST170000000000003", // Illinois
	"LASST250000000000003", // Massachusetts
	"LASST340000000000003", // New Jersey
	"LASST420000000000003", // Pennsylvania
	"LASST060000000000006", // California labor force
	"LASST360000000000006", // New York labor force
}

func (d *CPSLAUS) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing CPS/LAUS data")

	endYear := time.Now().Year()
	startYear := endYear - 2

	var allRows [][]any

	for _, seriesID := range lausSeries {
		url := fmt.Sprintf("https://api.bls.gov/publicAPI/v2/timeseries/data/%s?registrationkey=%s&startyear=%d&endyear=%d",
			seriesID, d.cfg.Fedsync.BLSKey, startYear, endYear)

		body, err := f.Download(ctx, url)
		if err != nil {
			log.Warn("skip series", zap.String("series", seriesID), zap.Error(err))
			continue
		}

		data, err := io.ReadAll(body)
		body.Close()
		if err != nil {
			continue
		}

		var resp blsSeriesResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}

		for _, series := range resp.Results.Series {
			for _, dp := range series.Data {
				allRows = append(allRows, []any{
					series.SeriesID,
					parseInt16Or(dp.Year, 0),
					dp.Period,
					parseFloat64Or(dp.Value, 0),
				})
			}
		}
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        d.Table(),
		Columns:      []string{"series_id", "year", "period", "value"},
		ConflictKeys: []string{"series_id", "year", "period"},
	}, allRows)
	if err != nil {
		return nil, eris.Wrap(err, "cps_laus: upsert")
	}

	log.Info("cps_laus sync complete", zap.Int64("rows", n))
	return &SyncResult{RowsSynced: n}, nil
}
