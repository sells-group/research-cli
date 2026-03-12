package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

var bpsCols = []string{
	"year", "state_fips", "county_fips", "county_name",
	"total_permits", "one_unit_permits", "two_unit_permits",
	"three_four_unit_permits", "five_plus_unit_permits", "total_valuation",
}

var bpsConflictKeys = []string{"year", "state_fips", "county_fips"}

const bpsBatchSize = 5000

// BuildingPermits syncs Census Building Permits Survey annual county data.
type BuildingPermits struct {
	cfg     *config.Config
	baseURL string // override for testing
}

// Name implements Dataset.
func (d *BuildingPermits) Name() string { return "building_permits" }

// Table implements Dataset.
func (d *BuildingPermits) Table() string { return "fed_data.building_permits" }

// Phase implements Dataset.
func (d *BuildingPermits) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *BuildingPermits) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *BuildingPermits) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

// Sync fetches and loads Census Building Permits Survey county data.
func (d *BuildingPermits) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("starting building_permits sync")

	csvPath := filepath.Join(tempDir, "bps_county.csv")
	var year int

	if d.baseURL != "" {
		year = time.Now().Year() - 1
		if _, err := f.DownloadToFile(ctx, d.baseURL, csvPath); err != nil {
			return nil, eris.Wrap(err, "building_permits: download")
		}
	} else {
		// Try recent years, falling back to older ones (Census data lags 1-2 years).
		var dlErr error
		for offset := 1; offset <= 4; offset++ {
			y := time.Now().Year() - offset
			url := fmt.Sprintf("https://www2.census.gov/econ/bps/County/co%da.txt", y)
			if _, dlErr = f.DownloadToFile(ctx, url, csvPath); dlErr == nil {
				year = y
				log.Info("found building permits file", zap.Int("year", year))
				break
			}
		}
		if dlErr != nil {
			return nil, eris.Wrap(dlErr, "building_permits: download (tried 4 years)")
		}
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "building_permits: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // BPS files have inconsistent field counts

	// BPS files have a two-row header (categories + sub-categories) plus a
	// blank line before data. Skip all three.
	for i := 0; i < 3; i++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				// File has only headers, no data rows.
				return &SyncResult{RowsSynced: 0}, nil
			}
			return nil, eris.Wrapf(err, "building_permits: skip header row %d", i+1)
		}
	}

	// Fixed column positions per Census BPS county annual format:
	// 0:Year  1:StateFIPS  2:CountyFIPS  3:RegionCode  4:DivCode  5:CountyName
	// 6:1-unit Bldgs  7:1-unit Units  8:1-unit Value
	// 9:2-units Bldgs  10:2-units Units  11:2-units Value
	// 12:3-4 units Bldgs  13:3-4 units Units  14:3-4 units Value
	// 15:5+ units Bldgs  16:5+ units Units  17:5+ units Value
	const (
		colStateFIPS  = 1
		colCountyFIPS = 2
		colCountyName = 5
		colOneUnit    = 7
		colOneVal     = 8
		colTwoUnit    = 10
		colTwoVal     = 11
		colThreeFour  = 13
		colThreeFVal  = 14
		colFivePlus   = 16
		colFivePVal   = 17
	)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      bpsCols,
			ConflictKeys: bpsConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "building_permits: upsert")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for {
		row, rErr := reader.Read()
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return nil, eris.Wrap(rErr, "building_permits: read row")
		}

		stateFIPS := strings.TrimSpace(safeGet(row, colStateFIPS))
		countyFIPS := strings.TrimSpace(safeGet(row, colCountyFIPS))
		if stateFIPS == "" || countyFIPS == "" {
			continue
		}

		// Pad FIPS codes.
		stateFIPS = padLeft(stateFIPS, 2)
		countyFIPS = padLeft(countyFIPS, 3)

		countyName := strings.TrimSpace(safeGet(row, colCountyName))

		oneUnit := parseIntOr(cleanNumeric(safeGet(row, colOneUnit)), 0)
		twoUnit := parseIntOr(cleanNumeric(safeGet(row, colTwoUnit)), 0)
		threeFour := parseIntOr(cleanNumeric(safeGet(row, colThreeFour)), 0)
		fivePlus := parseIntOr(cleanNumeric(safeGet(row, colFivePlus)), 0)
		total := oneUnit + twoUnit + threeFour + fivePlus

		oneVal := parseInt64Or(cleanNumeric(safeGet(row, colOneVal)), 0)
		twoVal := parseInt64Or(cleanNumeric(safeGet(row, colTwoVal)), 0)
		threeFourVal := parseInt64Or(cleanNumeric(safeGet(row, colThreeFVal)), 0)
		fivePlusVal := parseInt64Or(cleanNumeric(safeGet(row, colFivePVal)), 0)
		valuation := oneVal + twoVal + threeFourVal + fivePlusVal

		batch = append(batch, []any{
			year, stateFIPS, countyFIPS, countyName,
			total, oneUnit, twoUnit, threeFour, fivePlus, valuation,
		})

		if len(batch) >= bpsBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("building_permits sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}

// padLeft pads s with leading zeros to the specified width.
func padLeft(s string, width int) string {
	for len(s) < width {
		s = "0" + s
	}
	return s
}

// cleanNumeric strips commas from a numeric string.
func cleanNumeric(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), ",", "")
}
