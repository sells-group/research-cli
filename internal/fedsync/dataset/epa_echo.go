package dataset

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fetcher"
)

const epaEchoURL = "https://ordsext.epa.gov/FLA/www3/state_files/national_single.zip"

// EPAECHO syncs EPA ECHO facility data.
type EPAECHO struct{}

// Name implements Dataset.
func (d *EPAECHO) Name() string { return "epa_echo" }

// Table implements Dataset.
func (d *EPAECHO) Table() string { return "fed_data.epa_facilities" }

// Phase implements Dataset.
func (d *EPAECHO) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *EPAECHO) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *EPAECHO) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads EPA ECHO facility data.
func (d *EPAECHO) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("downloading EPA ECHO data")

	zipPath := filepath.Join(tempDir, "epa_echo.zip")
	if _, err := f.DownloadToFile(ctx, epaEchoURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "epa_echo: download")
	}

	extractDir := filepath.Join(tempDir, "epa_extract")
	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "epa_echo: extract zip")
	}

	// Find the first CSV file in the extracted contents
	var csvPath string
	for _, fp := range files {
		if strings.HasSuffix(strings.ToLower(fp), ".csv") {
			csvPath = fp
			break
		}
	}
	if csvPath == "" {
		return nil, eris.New("epa_echo: no CSV found in ZIP")
	}

	csvFile, err := openFileForRead(csvPath)
	if err != nil {
		return nil, eris.Wrap(err, "epa_echo: open csv")
	}
	defer csvFile.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, csvFile, fetcher.CSVOptions{HasHeader: false})

	var rows [][]any
	const batchSize = 5000
	var totalRows int64

	// Read the header row to build column index (case-insensitive via mapColumns)
	var colIdx map[string]int
	firstRow := true

	for row := range rowCh {
		if firstRow {
			// Strip quotes from header fields before mapping
			cleaned := make([]string, len(row))
			for i, col := range row {
				cleaned[i] = trimQuotes(col)
			}
			colIdx = mapColumns(cleaned)
			firstRow = false
			continue
		}

		regID := trimQuotes(getCol(row, colIdx, "registry_id"))
		if regID == "" {
			continue
		}

		rows = append(rows, []any{
			regID,
			trimQuotes(getCol(row, colIdx, "primary_name")),
			trimQuotes(getCol(row, colIdx, "city_name")),
			trimQuotes(getCol(row, colIdx, "state_code")),
			trimQuotes(getCol(row, colIdx, "postal_code")),
			parseFloat64Or(getCol(row, colIdx, "latitude83"), 0),
			parseFloat64Or(getCol(row, colIdx, "longitude83"), 0),
		})

		if len(rows) >= batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      []string{"registry_id", "fac_name", "fac_city", "fac_state", "fac_zip", "fac_lat", "fac_long"},
				ConflictKeys: []string{"registry_id"},
			}, rows)
			if err != nil {
				return nil, eris.Wrap(err, "epa_echo: upsert")
			}
			totalRows += n
			rows = rows[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "epa_echo: stream csv")
	}

	if len(rows) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      []string{"registry_id", "fac_name", "fac_city", "fac_state", "fac_zip", "fac_lat", "fac_long"},
			ConflictKeys: []string{"registry_id"},
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "epa_echo: upsert final")
		}
		totalRows += n
	}

	log.Info("epa_echo sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}
