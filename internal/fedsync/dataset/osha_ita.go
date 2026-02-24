package dataset

import (
	"context"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fetcher"
)

const oshaURL = "https://www.osha.gov/severeinjury/xml/severeinjury.zip"

// OSHITA syncs OSHA ITA (Injury Tracking Application) inspection data.
type OSHITA struct{}

// Name implements Dataset.
func (d *OSHITA) Name() string { return "osha_ita" }

// Table implements Dataset.
func (d *OSHITA) Table() string { return "fed_data.osha_inspections" }

// Phase implements Dataset.
func (d *OSHITA) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *OSHITA) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *OSHITA) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

// Sync fetches and loads OSHA ITA inspection data.
func (d *OSHITA) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("downloading OSHA ITA data")

	zipPath := filepath.Join(tempDir, "osha_ita.zip")
	if _, err := f.DownloadToFile(ctx, oshaURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "osha_ita: download")
	}

	extractedPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "osha_ita: extract zip")
	}

	log.Info("parsing OSHA data", zap.String("path", extractedPath))

	csvFile, err := openFileForRead(extractedPath)
	if err != nil {
		return nil, eris.Wrap(err, "osha_ita: open csv")
	}
	defer csvFile.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, csvFile, fetcher.CSVOptions{HasHeader: true})

	var rows [][]any
	const batchSize = 5000
	var totalRows int64

	for row := range rowCh {
		if len(row) < 12 {
			continue
		}
		rows = append(rows, []any{
			parseInt64Or(row[0], 0),    // activity_nr
			trimQuotes(row[1]),         // estab_name
			trimQuotes(row[2]),         // site_city
			trimQuotes(row[3]),         // site_state
			trimQuotes(row[4]),         // site_zip
			trimQuotes(row[5]),         // naics_code
			trimQuotes(row[6]),         // sic_code
			trimQuotes(row[7]),         // open_date
			trimQuotes(row[8]),         // close_case_date
			firstChar(row[9]),          // case_type
			firstChar(row[10]),         // safety_hlth
			parseFloat64Or(row[11], 0), // total_penalty
		})

		if len(rows) >= batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      []string{"activity_nr", "estab_name", "site_city", "site_state", "site_zip", "naics_code", "sic_code", "open_date", "close_case_date", "case_type", "safety_hlth", "total_penalty"},
				ConflictKeys: []string{"activity_nr"},
			}, rows)
			if err != nil {
				return nil, eris.Wrap(err, "osha_ita: upsert")
			}
			totalRows += n
			rows = rows[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "osha_ita: stream csv")
	}

	if len(rows) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      []string{"activity_nr", "estab_name", "site_city", "site_state", "site_zip", "naics_code", "sic_code", "open_date", "close_case_date", "case_type", "safety_hlth", "total_penalty"},
			ConflictKeys: []string{"activity_nr"},
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "osha_ita: upsert final")
		}
		totalRows += n
	}

	return &SyncResult{RowsSynced: totalRows}, nil
}
