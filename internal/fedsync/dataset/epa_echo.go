package dataset

import (
	"context"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const epaEchoURL = "https://echo.epa.gov/files/echodownloads/frs_downloads/NATIONAL_FACILITY_FILE.CSV.zip"

// EPAECHO syncs EPA ECHO facility data.
type EPAECHO struct{}

func (d *EPAECHO) Name() string    { return "epa_echo" }
func (d *EPAECHO) Table() string   { return "fed_data.epa_facilities" }
func (d *EPAECHO) Phase() Phase    { return Phase2 }
func (d *EPAECHO) Cadence() Cadence { return Monthly }

func (d *EPAECHO) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *EPAECHO) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("downloading EPA ECHO data")

	zipPath := filepath.Join(tempDir, "epa_echo.zip")
	if _, err := f.DownloadToFile(ctx, epaEchoURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "epa_echo: download")
	}

	extractedPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "epa_echo: extract zip")
	}

	csvFile, err := openFileForRead(extractedPath)
	if err != nil {
		return nil, eris.Wrap(err, "epa_echo: open csv")
	}
	defer csvFile.Close()

	rowCh, errCh := fetcher.StreamCSV(ctx, csvFile, fetcher.CSVOptions{HasHeader: true})

	var rows [][]any
	const batchSize = 5000
	var totalRows int64

	for row := range rowCh {
		if len(row) < 9 {
			continue
		}
		rows = append(rows, []any{
			trimQuotes(row[0]),                // registry_id
			trimQuotes(row[1]),                // fac_name
			trimQuotes(row[2]),                // fac_city
			trimQuotes(row[3]),                // fac_state
			trimQuotes(row[4]),                // fac_zip
			parseFloat64Or(row[7], 0),         // fac_lat
			parseFloat64Or(row[8], 0),         // fac_long
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

	return &SyncResult{RowsSynced: totalRows}, nil
}
