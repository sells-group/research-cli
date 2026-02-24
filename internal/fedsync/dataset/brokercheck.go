package dataset

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fetcher"
)

const brokerCheckURL = "https://files.brokercheck.finra.org/firm/firm.zip"

// BrokerCheck syncs FINRA BrokerCheck firm data (pipe-delimited).
type BrokerCheck struct{}

// Name implements Dataset.
func (d *BrokerCheck) Name() string { return "brokercheck" }

// Table implements Dataset.
func (d *BrokerCheck) Table() string { return "fed_data.brokercheck" }

// Phase implements Dataset.
func (d *BrokerCheck) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *BrokerCheck) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *BrokerCheck) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads FINRA BrokerCheck firm data.
func (d *BrokerCheck) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("downloading BrokerCheck data")

	zipPath := filepath.Join(tempDir, "brokercheck_firm.zip")
	if _, err := f.DownloadToFile(ctx, brokerCheckURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "brokercheck: download zip")
	}

	extractedPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "brokercheck: extract zip")
	}

	log.Info("parsing BrokerCheck file", zap.String("path", extractedPath))

	// BrokerCheck firm file is pipe-delimited. Parse and batch upsert.
	// Fields: CRD|Firm Name|SEC Number|Main Address City|Main Address State|...
	csvOpts := fetcher.CSVOptions{Delimiter: '|', HasHeader: true, LazyQuotes: true}
	headerCh := make(chan []string, 1)
	csvOpts.HeaderCh = headerCh

	file, err := openFileForRead(extractedPath)
	if err != nil {
		return nil, eris.Wrap(err, "brokercheck: open extracted file")
	}
	defer file.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, file, csvOpts)

	var rows [][]any
	const batchSize = 5000
	var totalRows int64

	for row := range rowCh {
		if len(row) < 7 {
			continue
		}
		rows = append(rows, []any{
			parseIntOr(row[0], 0), // crd_number
			trimQuotes(row[1]),    // firm_name
			trimQuotes(row[2]),    // sec_number
			trimQuotes(row[3]),    // main_addr_city
			trimQuotes(row[4]),    // main_addr_state
			parseIntOr(row[5], 0), // num_branch_offices
			parseIntOr(row[6], 0), // num_registered_reps
		})

		if len(rows) >= batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.brokercheck",
				Columns:      []string{"crd_number", "firm_name", "sec_number", "main_addr_city", "main_addr_state", "num_branch_offices", "num_registered_reps"},
				ConflictKeys: []string{"crd_number"},
			}, rows)
			if err != nil {
				return nil, eris.Wrap(err, "brokercheck: upsert batch")
			}
			totalRows += n
			rows = rows[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "brokercheck: stream CSV")
	}

	if len(rows) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.brokercheck",
			Columns:      []string{"crd_number", "firm_name", "sec_number", "main_addr_city", "main_addr_state", "num_branch_offices", "num_registered_reps"},
			ConflictKeys: []string{"crd_number"},
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "brokercheck: upsert final batch")
		}
		totalRows += n
	}

	log.Info("brokercheck sync complete", zap.Int64("rows", totalRows))
	return &SyncResult{RowsSynced: totalRows}, nil
}

func openFileForRead(path string) (*os.File, error) {
	return os.Open(path)
}
