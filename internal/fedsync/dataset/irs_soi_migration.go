package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

var irsMigrationCols = []string{
	"year", "direction",
	"state_fips_origin", "county_fips_origin",
	"state_fips_dest", "county_fips_dest",
	"num_returns", "num_exemptions", "adjusted_gross_income",
}

var irsMigrationConflictKeys = []string{
	"year", "direction",
	"state_fips_origin", "county_fips_origin",
	"state_fips_dest", "county_fips_dest",
}

const irsBatchSize = 5000

// IRSSOIMigration syncs IRS Statistics of Income county-to-county migration data.
type IRSSOIMigration struct {
	baseURL string // override for testing
}

// Name implements Dataset.
func (d *IRSSOIMigration) Name() string { return "irs_soi_migration" }

// Table implements Dataset.
func (d *IRSSOIMigration) Table() string { return "fed_data.irs_soi_migration" }

// Phase implements Dataset.
func (d *IRSSOIMigration) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *IRSSOIMigration) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *IRSSOIMigration) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.July)
}

// Sync fetches and loads IRS SOI county migration flow data.
func (d *IRSSOIMigration) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("starting irs_soi_migration sync")

	// Determine year pair: e.g., for 2021→2022, use "2122".
	// Try recent pairs, falling back to older ones (IRS data lags 1-2 years).
	y := time.Now().Year()
	var pairs []string
	for offset := 1; offset <= 4; offset++ {
		y2 := y - offset
		y1 := y2 - 1
		pairs = append(pairs, fmt.Sprintf("%02d%02d", y1%100, y2%100))
	}

	var totalRows int64

	for _, direction := range []string{"inflow", "outflow"} {
		prefix := "countyinflow"
		if direction == "outflow" {
			prefix = "countyoutflow"
		}

		var csvPath string
		var dlYear int
		if d.baseURL != "" {
			csvPath = filepath.Join(tempDir, fmt.Sprintf("irs_%s.csv", direction))
			if _, err := f.DownloadToFile(ctx, d.baseURL, csvPath); err != nil {
				return nil, eris.Wrapf(err, "irs_soi_migration: download %s", direction)
			}
			dlYear = y - 1
		} else {
			var dlErr error
			for i, pair := range pairs {
				url := fmt.Sprintf("https://www.irs.gov/pub/irs-soi/%s%s.csv", prefix, pair)
				csvPath = filepath.Join(tempDir, fmt.Sprintf("irs_%s.csv", direction))
				if _, dlErr = f.DownloadToFile(ctx, url, csvPath); dlErr == nil {
					dlYear = y - 1 - i
					log.Info("found IRS SOI file", zap.String("pair", pair), zap.String("direction", direction))
					break
				}
			}
			if dlErr != nil {
				return nil, eris.Wrapf(dlErr, "irs_soi_migration: download %s (tried pairs %v)", direction, pairs)
			}
		}

		n, err := d.parseFile(ctx, pool, csvPath, direction, dlYear, log)
		if err != nil {
			return nil, eris.Wrapf(err, "irs_soi_migration: parse %s", direction)
		}
		totalRows += n
	}

	log.Info("irs_soi_migration sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}

func (d *IRSSOIMigration) parseFile(ctx context.Context, pool db.Pool, csvPath, direction string, year int, log *zap.Logger) (int64, error) {
	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return 0, eris.Wrap(err, "irs_soi_migration: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "irs_soi_migration: read header")
	}
	colIdx := mapColumns(header)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.irs_soi_migration",
			Columns:      irsMigrationCols,
			ConflictKeys: irsMigrationConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "irs_soi_migration: upsert")
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
			return 0, eris.Wrap(rErr, "irs_soi_migration: read row")
		}

		originState := getCol(row, colIdx, "y1_statefips")
		originCounty := getCol(row, colIdx, "y1_countyfips")
		destState := getCol(row, colIdx, "y2_statefips")
		destCounty := getCol(row, colIdx, "y2_countyfips")

		// Skip summary rows: county FIPS "000" or state totals.
		if originCounty == "000" || destCounty == "000" {
			continue
		}
		if originState == "96" || originState == "97" || originState == "98" {
			continue
		}
		if destState == "96" || destState == "97" || destState == "98" {
			continue
		}

		numReturns := parseIntOr(getCol(row, colIdx, "return_num"), 0)
		numExemptions := parseIntOr(getCol(row, colIdx, "exmpt_num"), 0)
		agi := parseInt64Or(getCol(row, colIdx, "adjusted_gross_income"), 0)

		batch = append(batch, []any{
			year, direction,
			originState, originCounty,
			destState, destCounty,
			numReturns, numExemptions, agi,
		})

		if len(batch) >= irsBatchSize {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}

	log.Info("irs_soi_migration direction synced",
		zap.String("direction", direction),
		zap.Int64("rows", totalRows),
	)
	return totalRows, nil
}
