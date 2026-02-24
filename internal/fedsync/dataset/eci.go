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

// ECI syncs BLS Employment Cost Index data.
type ECI struct {
	cfg *config.Config
}

func (d *ECI) Name() string     { return "eci" }
func (d *ECI) Table() string    { return "fed_data.eci_data" }
func (d *ECI) Phase() Phase     { return Phase2 }
func (d *ECI) Cadence() Cadence { return Quarterly }

func (d *ECI) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, 2)
}

// blsSeriesResponse is the BLS API v2 response format.
type blsSeriesResponse struct {
	Status  string `json:"status"`
	Results struct {
		Series []struct {
			SeriesID string `json:"seriesID"`
			Data     []struct {
				Year   string `json:"year"`
				Period string `json:"period"`
				Value  string `json:"value"`
			} `json:"data"`
		} `json:"series"`
	} `json:"Results"`
}

// ECI target series: total compensation, wages/salaries, benefits.
var eciSeries = []string{
	"CIU1010000000000A", // Total compensation, all workers
	"CIU1020000000000A", // Wages and salaries
	"CIU1030000000000A", // Benefits
	"CIU2010000520000A", // Finance and insurance
	"CIU2010000540000A", // Professional and business services
}

func (d *ECI) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing ECI data")

	endYear := time.Now().Year()
	startYear := endYear - 3

	_ = fmt.Sprintf("https://api.bls.gov/publicAPI/v2/timeseries/data/?registrationkey=%s&startyear=%d&endyear=%d",
		d.cfg.Fedsync.BLSKey, startYear, endYear)

	// BLS API requires POST with series IDs in body.
	reqBody := map[string]any{
		"seriesid":  eciSeries,
		"startyear": fmt.Sprintf("%d", startYear),
		"endyear":   fmt.Sprintf("%d", endYear),
	}
	_ = reqBody

	// For simplicity, use GET per series.
	var allRows [][]any

	for _, seriesID := range eciSeries {
		seriesURL := fmt.Sprintf("https://api.bls.gov/publicAPI/v2/timeseries/data/%s?registrationkey=%s&startyear=%d&endyear=%d",
			seriesID, d.cfg.Fedsync.BLSKey, startYear, endYear)

		body, err := f.Download(ctx, seriesURL)
		if err != nil {
			log.Warn("skip series", zap.String("series", seriesID), zap.Error(err))
			continue
		}

		data, err := io.ReadAll(body)
		_ = body.Close()
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
		return nil, eris.Wrap(err, "eci: upsert")
	}

	log.Info("eci sync complete", zap.Int64("rows", n))
	return &SyncResult{RowsSynced: n}, nil
}
