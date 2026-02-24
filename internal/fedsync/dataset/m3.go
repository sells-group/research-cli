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

// Name implements Dataset.
func (d *M3) Name() string { return "m3" }

// Table implements Dataset.
func (d *M3) Table() string { return "fed_data.m3_data" }

// Phase implements Dataset.
func (d *M3) Phase() Phase { return Phase3 }

// Cadence implements Dataset.
func (d *M3) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *M3) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// m3DataTypes maps data_type_code values to human-readable names.
// The consolidated eits/m3 endpoint returns these in the response.
var m3DataTypes = map[string]string{
	"NO": "new_orders",
	"VS": "shipments",
	"TI": "inventories",
	"UO": "unfilled_orders",
}

// Sync fetches and loads Census M3 manufacturers' survey data.
func (d *M3) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing M3 data")

	// Census consolidated M3 endpoint requires: time, seasonally_adj, for=us:*
	// Fetch all data types and category codes in a single request.
	url := fmt.Sprintf(
		"https://api.census.gov/data/timeseries/eits/m3?get=cell_value,time_slot_id,category_code,data_type_code&for=us:*&time=from+2020&seasonally_adj=yes&key=%s",
		d.cfg.Fedsync.CensusKey)

	body, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "m3: download")
	}

	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, eris.Wrap(err, "m3: read response")
	}

	var result [][]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, eris.Wrap(err, "m3: parse json")
	}

	if len(result) < 2 {
		return &SyncResult{RowsSynced: 0}, nil
	}

	// Build column index from header
	header := result[0]
	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[col] = i
	}

	var allRows [][]any
	seen := make(map[string]int)

	for _, row := range result[1:] {
		cellValue := getColIdx(row, colIdx, "cell_value")
		timeStr := getColIdx(row, colIdx, "time")
		catCode := getColIdx(row, colIdx, "category_code")
		dtCode := getColIdx(row, colIdx, "data_type_code")

		// Only keep core data types (VS, NO, TI, UO)
		dataType, ok := m3DataTypes[dtCode]
		if !ok {
			continue
		}

		year, month := parseTimeSlot(timeStr)
		if year == 0 {
			continue
		}

		r := []any{
			catCode,
			dataType,
			int16(year),
			int16(month),
			parseInt64Or(cellValue, 0),
		}

		key := fmt.Sprintf("%s|%s|%d|%d", catCode, dataType, year, month)
		if idx, exists := seen[key]; exists {
			allRows[idx] = r
			continue
		}
		seen[key] = len(allRows)
		allRows = append(allRows, r)
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
