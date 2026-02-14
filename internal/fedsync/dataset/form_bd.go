package dataset

import (
	"context"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const formBDURL = "https://www.sec.gov/files/data/broker-dealer-data/bd_firm.zip"

// FormBD syncs FINRA/EDGAR Form BD broker-dealer registrations.
type FormBD struct {
	cfg *config.Config
}

func (d *FormBD) Name() string    { return "form_bd" }
func (d *FormBD) Table() string   { return "fed_data.form_bd" }
func (d *FormBD) Phase() Phase    { return Phase2 }
func (d *FormBD) Cadence() Cadence { return Monthly }

func (d *FormBD) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *FormBD) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("downloading Form BD data")

	zipPath := filepath.Join(tempDir, "form_bd.zip")
	if _, err := f.DownloadToFile(ctx, formBDURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "form_bd: download")
	}

	extractedPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "form_bd: extract zip")
	}

	log.Info("parsing Form BD data", zap.String("path", extractedPath))

	csvFile, err := openFileForRead(extractedPath)
	if err != nil {
		return nil, eris.Wrap(err, "form_bd: open csv")
	}
	defer csvFile.Close()

	// Form BD file is pipe-delimited with header row.
	rowCh, errCh := fetcher.StreamCSV(ctx, csvFile, fetcher.CSVOptions{
		Delimiter: '|',
		HasHeader: true,
	})

	columns := []string{"crd_number", "sec_number", "firm_name", "city", "state", "fiscal_year_end", "num_reps"}
	conflictKeys := []string{"crd_number"}

	var rows [][]any
	const batchSize = 5000
	var totalRows int64

	for row := range rowCh {
		if len(row) < 7 {
			continue
		}
		rows = append(rows, []any{
			parseIntOr(row[0], 0), // crd_number
			trimQuotes(row[1]),    // sec_number
			trimQuotes(row[2]),    // firm_name
			trimQuotes(row[3]),    // city
			trimQuotes(row[4]),    // state
			trimQuotes(row[5]),    // fiscal_year_end
			parseIntOr(row[6], 0), // num_reps
		})

		if len(rows) >= batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, rows)
			if err != nil {
				return nil, eris.Wrap(err, "form_bd: upsert")
			}
			totalRows += n
			rows = rows[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "form_bd: stream csv")
	}

	if len(rows) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "form_bd: upsert final")
		}
		totalRows += n
	}

	log.Info("form_bd sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}
