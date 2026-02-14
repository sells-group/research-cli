package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// ABS syncs Census Annual Business Survey data.
type ABS struct {
	cfg *config.Config
}

func (d *ABS) Name() string    { return "abs" }
func (d *ABS) Table() string   { return "fed_data.abs_data" }
func (d *ABS) Phase() Phase    { return Phase3 }
func (d *ABS) Cadence() Cadence { return Annual }

func (d *ABS) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

func (d *ABS) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing ABS data")

	year := time.Now().Year() - 1
	url := fmt.Sprintf("https://api.census.gov/data/%d/abscs?get=NAICS2017,GEO_ID,FIRMPDEMP,RCPPDEMP,PAYANN&for=us:*&key=%s",
		year, d.cfg.Fedsync.CensusKey)

	body, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "abs: download census api")
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "abs: read response")
	}

	var result [][]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, eris.Wrap(err, "abs: parse json")
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
			row[0],                  // naics
			row[1],                  // geo_id
			parseIntOr(row[2], 0),   // firmpdemp
			parseInt64Or(row[3], 0), // rcppdemp
			parseInt64Or(row[4], 0), // payann
		})
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        d.Table(),
		Columns:      []string{"year", "naics", "geo_id", "firmpdemp", "rcppdemp", "payann"},
		ConflictKeys: []string{"year", "naics", "geo_id"},
	}, rows)
	if err != nil {
		return nil, eris.Wrap(err, "abs: upsert")
	}

	return &SyncResult{RowsSynced: n}, nil
}
