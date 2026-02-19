package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// ASM syncs Census Annual Survey of Manufactures data.
type ASM struct {
	cfg *config.Config
}

func (d *ASM) Name() string     { return "asm" }
func (d *ASM) Table() string    { return "fed_data.asm_data" }
func (d *ASM) Phase() Phase     { return Phase2 }
func (d *ASM) Cadence() Cadence { return Annual }

func (d *ASM) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

func (d *ASM) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing ASM data")

	// Try years from most recent backward until we find available data
	// (Census data lags 1-2 years; the latest year may not be published yet)
	for year := time.Now().Year() - 1; year >= 2020; year-- {
		url := fmt.Sprintf("https://api.census.gov/data/%d/asm/product?get=NAICS2017,GEO_ID,VALADD,TOTVAL_SHIP,PRODWRKRS&for=us:*&key=%s",
			year, d.cfg.Fedsync.CensusKey)

		body, err := f.Download(ctx, url)
		if err != nil {
			if strings.Contains(err.Error(), "status 404") || strings.Contains(err.Error(), "status 400") {
				log.Info("ASM data not available for year, trying earlier", zap.Int("year", year))
				continue
			}
			return nil, eris.Wrapf(err, "asm: download census api year %d", year)
		}

		data, err := io.ReadAll(body)
		body.Close()
		if err != nil {
			return nil, eris.Wrap(err, "asm: read response")
		}

		var result [][]string
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, eris.Wrap(err, "asm: parse json")
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
				parseInt64Or(row[2], 0), // valadd
				parseInt64Or(row[3], 0), // totval_ship
				parseIntOr(row[4], 0),   // prodwrkrs
			})
		}

		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      []string{"year", "naics", "geo_id", "valadd", "totval_ship", "prodwrkrs"},
			ConflictKeys: []string{"year", "naics", "geo_id"},
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "asm: upsert")
		}

		return &SyncResult{RowsSynced: n, Metadata: map[string]any{"year": year}}, nil
	}

	log.Warn("ASM: no data available for any year from 2020 to present")
	return &SyncResult{RowsSynced: 0}, nil
}
