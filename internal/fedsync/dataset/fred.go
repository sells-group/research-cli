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

// FRED syncs Federal Reserve Economic Data series.
type FRED struct {
	cfg *config.Config
}

func (d *FRED) Name() string     { return "fred" }
func (d *FRED) Table() string    { return "fed_data.fred_series" }
func (d *FRED) Phase() Phase     { return Phase3 }
func (d *FRED) Cadence() Cadence { return Monthly }

func (d *FRED) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Target FRED series for financial advisory industry analysis.
var fredTargetSeries = []string{
	"GDP",      // Gross Domestic Product
	"UNRATE",   // Unemployment Rate
	"CPIAUCSL", // Consumer Price Index
	"FEDFUNDS", // Federal Funds Rate
	"GS10",     // 10-Year Treasury
	"GS2",      // 2-Year Treasury
	"T10Y2Y",   // 10Y-2Y Spread
	"SP500",    // S&P 500
	"VIXCLS",   // VIX Volatility
	"M2SL",     // M2 Money Supply
	"DTWEXBGS", // Trade Weighted US Dollar
	"HOUST",    // Housing Starts
	"RSAFS",    // Retail Sales
	"INDPRO",   // Industrial Production
	"PAYEMS",   // Nonfarm Payrolls
}

type fredResponse struct {
	Observations []struct {
		Date  string `json:"date"`
		Value string `json:"value"`
	} `json:"observations"`
}

func (d *FRED) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing FRED data")

	var allRows [][]any

	for _, seriesID := range fredTargetSeries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := fmt.Sprintf("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=120",
			seriesID, d.cfg.Fedsync.FREDKey)

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

		var resp fredResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}

		for _, obs := range resp.Observations {
			if obs.Value == "." {
				continue
			}
			allRows = append(allRows, []any{
				seriesID,
				obs.Date,
				parseFloat64Or(obs.Value, 0),
			})
		}
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        d.Table(),
		Columns:      []string{"series_id", "obs_date", "value"},
		ConflictKeys: []string{"series_id", "obs_date"},
	}, allRows)
	if err != nil {
		return nil, eris.Wrap(err, "fred: upsert")
	}

	log.Info("fred sync complete", zap.Int64("rows", n))
	return &SyncResult{RowsSynced: n}, nil
}
