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

// NES syncs Census Nonemployer Statistics data.
type NES struct {
	cfg *config.Config
}

func (d *NES) Name() string     { return "nes" }
func (d *NES) Table() string    { return "fed_data.nes_data" }
func (d *NES) Phase() Phase     { return Phase2 }
func (d *NES) Cadence() Cadence { return Annual }

func (d *NES) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

func (d *NES) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing NES data")

	year := time.Now().Year() - 1
	url := fmt.Sprintf("https://api.census.gov/data/%d/nonemp?get=NAICS2017,GEO_ID,FIRMPDEMP,RCPPDEMP,PAYANN_PCT&for=us:*&key=%s",
		year, d.cfg.Fedsync.CensusKey)

	body, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "nes: download census api")
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "nes: read response")
	}

	var result [][]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, eris.Wrap(err, "nes: parse json")
	}

	if len(result) < 2 {
		return &SyncResult{RowsSynced: 0}, nil
	}

	var rows [][]any
	for _, row := range result[1:] {
		if len(row) < 5 {
			continue
		}
		rows = append(rows, []any{
			int16(year),
			row[0], // naics
			row[1], // geo_id
			parseIntOr(row[2], 0),
			parseInt64Or(row[3], 0),
			parseFloat64Or(row[4], 0),
		})
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        d.Table(),
		Columns:      []string{"year", "naics", "geo_id", "firmpdemp", "rcppdemp", "payann_pct"},
		ConflictKeys: []string{"year", "naics", "geo_id"},
	}, rows)
	if err != nil {
		return nil, eris.Wrap(err, "nes: upsert")
	}

	return &SyncResult{RowsSynced: n}, nil
}
