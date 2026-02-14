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

// M3 syncs Census M3 survey data (manufacturers' shipments, inventories, orders).
type M3 struct {
	cfg *config.Config
}

func (d *M3) Name() string     { return "m3" }
func (d *M3) Table() string    { return "fed_data.m3_data" }
func (d *M3) Phase() Phase     { return Phase3 }
func (d *M3) Cadence() Cadence { return Monthly }

func (d *M3) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// m3Categories defines the M3 series to fetch.
var m3Categories = []struct {
	Category string
	DataType string
}{
	{"AMTMNO", "new_orders"},      // New Orders
	{"AMTMVS", "shipments"},       // Value of Shipments
	{"AMTMTI", "inventories"},     // Total Inventories
	{"AMTMUO", "unfilled_orders"}, // Unfilled Orders
}

func (d *M3) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing M3 data")

	var allRows [][]any

	for _, cat := range m3Categories {
		url := fmt.Sprintf("https://api.census.gov/data/timeseries/eits/%s?get=cell_value,time_slot_id,category_code,data_type_code&key=%s",
			cat.Category, d.cfg.Fedsync.CensusKey)

		body, err := f.Download(ctx, url)
		if err != nil {
			log.Warn("skip category", zap.String("category", cat.Category), zap.Error(err))
			continue
		}

		data, err := io.ReadAll(body)
		body.Close()
		if err != nil {
			continue
		}

		var result [][]string
		if err := json.Unmarshal(data, &result); err != nil {
			continue
		}

		for _, row := range result[1:] {
			if len(row) < 2 {
				continue
			}
			// Parse time_slot_id like "2024-01" into year/month.
			year, month := parseTimeSlot(row[1])
			if year == 0 {
				continue
			}
			allRows = append(allRows, []any{
				cat.Category,
				cat.DataType,
				int16(year),
				int16(month),
				parseInt64Or(row[0], 0),
			})
		}
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        d.Table(),
		Columns:      []string{"category", "data_type", "year", "month", "value"},
		ConflictKeys: []string{"category", "data_type", "year", "month"},
	}, allRows)
	if err != nil {
		return nil, eris.Wrap(err, "m3: upsert")
	}

	log.Info("m3 sync complete", zap.Int64("rows", n))
	return &SyncResult{RowsSynced: n}, nil
}

func parseTimeSlot(s string) (int, int) {
	if len(s) < 7 || s[4] != '-' {
		return 0, 0
	}
	year := parseIntOr(s[:4], 0)
	month := parseIntOr(s[5:7], 0)
	return year, month
}
