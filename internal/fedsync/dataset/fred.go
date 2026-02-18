package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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

	var mu sync.Mutex
	var allRows [][]any

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, seriesID := range fredTargetSeries {
		seriesID := seriesID
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			url := fmt.Sprintf("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=120",
				seriesID, d.cfg.Fedsync.FREDKey)

			body, err := f.Download(gctx, url)
			if err != nil {
				log.Warn("skip series", zap.String("series", seriesID), zap.Error(err))
				return nil
			}

			data, err := io.ReadAll(body)
			body.Close()
			if err != nil {
				return nil
			}

			var resp fredResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil
			}

			var rows [][]any
			for _, obs := range resp.Observations {
				if obs.Value == "." {
					continue
				}
				rows = append(rows, []any{
					seriesID,
					obs.Date,
					parseFloat64Or(obs.Value, 0),
				})
			}

			mu.Lock()
			allRows = append(allRows, rows...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, eris.Wrap(err, "fred: fetch series")
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
