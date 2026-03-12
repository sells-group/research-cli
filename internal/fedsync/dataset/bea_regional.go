package dataset

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// beaTables lists the BEA tables to download: GDP, personal income, per capita income.
var beaTables = []string{"CAGDP1", "CAINC1", "CAINC4"}

var beaCols = []string{
	"table_name", "geo_fips", "geo_name", "line_code", "description", "unit", "year", "value",
}

var beaConflictKeys = []string{"table_name", "geo_fips", "line_code", "year"}

const beaBatchSize = 5000

// BEARegional syncs BEA regional GDP and income data by county.
type BEARegional struct {
	cfg     *config.Config
	baseURL string // override for testing
}

// Name implements Dataset.
func (d *BEARegional) Name() string { return "bea_regional" }

// Table implements Dataset.
func (d *BEARegional) Table() string { return "fed_data.bea_regional" }

// Phase implements Dataset.
func (d *BEARegional) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *BEARegional) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *BEARegional) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.November)
}

// Sync fetches and loads BEA regional data for all configured tables.
func (d *BEARegional) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("starting bea_regional sync")

	var totalRows int64

	for _, table := range beaTables {
		n, err := d.syncTable(ctx, pool, f, tempDir, log, table)
		if err != nil {
			return nil, eris.Wrapf(err, "bea_regional: sync table %s", table)
		}
		totalRows += n
	}

	log.Info("bea_regional sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}

func (d *BEARegional) syncTable(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string, log *zap.Logger, table string) (int64, error) {
	url := d.baseURL
	if url == "" {
		url = "https://apps.bea.gov/regional/zip/" + table + ".zip"
	}

	zipPath := filepath.Join(tempDir, table+".zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return 0, eris.Wrapf(err, "bea_regional: download %s", table)
	}

	extractDir := filepath.Join(tempDir, table+"_extract")
	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return 0, eris.Wrapf(err, "bea_regional: extract %s", table)
	}

	// Find the main CSV file.
	var csvPath string
	for _, fp := range files {
		if strings.HasSuffix(strings.ToLower(fp), ".csv") {
			csvPath = fp
			break
		}
	}
	if csvPath == "" {
		return 0, eris.Errorf("bea_regional: no CSV found in %s ZIP", table)
	}

	csvFile, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return 0, eris.Wrapf(err, "bea_regional: open %s csv", table)
	}
	defer csvFile.Close() //nolint:errcheck

	reader := csv.NewReader(csvFile)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // BEA CSVs have inconsistent field counts
	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrapf(err, "bea_regional: read %s header", table)
	}

	// Identify year columns (header values that parse as integers >= 1900).
	type yearCol struct {
		idx  int
		year int
	}
	var yearCols []yearCol
	colIdx := make(map[string]int, len(header))
	for i, h := range header {
		h = strings.TrimSpace(h)
		colIdx[strings.ToLower(h)] = i
		if y, err := strconv.Atoi(h); err == nil && y >= 1900 {
			yearCols = append(yearCols, yearCol{idx: i, year: y})
		}
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      beaCols,
			ConflictKeys: beaConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "bea_regional: upsert")
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
			return 0, eris.Wrapf(rErr, "bea_regional: read %s row", table)
		}

		geoFIPS := strings.TrimSpace(safeGet(row, colIdx["geofips"]))
		geoFIPS = strings.Trim(geoFIPS, `"`)
		// Filter: keep only county (5-digit) and state (2-digit) FIPS.
		fLen := len(geoFIPS)
		if fLen != 2 && fLen != 5 {
			continue
		}

		geoName := sanitizeUTF8(strings.TrimSpace(safeGet(row, colIdx["geoname"])))
		geoName = strings.Trim(geoName, `"`)
		lineCodeStr := strings.TrimSpace(safeGet(row, colIdx["linecode"]))
		lineCode, err := strconv.Atoi(lineCodeStr)
		if err != nil {
			continue
		}
		desc := sanitizeUTF8(strings.TrimSpace(safeGet(row, colIdx["description"])))
		desc = strings.Trim(desc, `"`)
		unit := strings.TrimSpace(safeGet(row, colIdx["unit"]))
		unit = strings.Trim(unit, `"`)

		for _, yc := range yearCols {
			valStr := strings.TrimSpace(safeGet(row, yc.idx))
			valStr = strings.Trim(valStr, `"`)
			if valStr == "" || valStr == "(NA)" || valStr == "(D)" || valStr == "(L)" || valStr == "(T)" {
				continue
			}
			// Strip commas for numeric parsing.
			valStr = strings.ReplaceAll(valStr, ",", "")
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				continue
			}

			batch = append(batch, []any{
				table, geoFIPS, geoName, lineCode, desc, unit, yc.year, val,
			})

			if len(batch) >= beaBatchSize {
				if err := flush(); err != nil {
					return 0, err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}

	log.Info("bea table synced", zap.String("table", table), zap.Int64("rows", totalRows))
	return totalRows, nil
}

// safeGet safely gets a column value from a row by index.
func safeGet(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return row[idx]
}
