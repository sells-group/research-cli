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
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	susbStartYear = 2019
	susbBatchSize = 5000
)

// SUSB implements the Census Statistics of US Businesses dataset.
type SUSB struct{}

// Name implements Dataset.
func (d *SUSB) Name() string { return "susb" }

// Table implements Dataset.
func (d *SUSB) Table() string { return "fed_data.susb_data" }

// Phase implements Dataset.
func (d *SUSB) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *SUSB) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *SUSB) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

// Sync fetches and loads Census SUSB business data.
func (d *SUSB) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "susb"))
	var totalRows int64

	currentYear := time.Now().Year() - 1

	for year := susbStartYear; year <= currentYear; year++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Census now publishes SUSB as plain TXT files (not ZIPs).
		url := fmt.Sprintf("https://www2.census.gov/programs-surveys/susb/datasets/%d/us_state_6digitnaics_%d.txt", year, year)
		log.Info("downloading SUSB data", zap.Int("year", year), zap.String("url", url))

		txtPath := filepath.Join(tempDir, fmt.Sprintf("susb_%d.txt", year))
		if _, err := f.DownloadToFile(ctx, url, txtPath); err != nil {
			if strings.Contains(err.Error(), "status 404") {
				log.Info("SUSB data not yet available, skipping", zap.Int("year", year))
				continue
			}
			return nil, eris.Wrapf(err, "susb: download year %d", year)
		}

		file, err := os.Open(txtPath)
		if err != nil {
			return nil, eris.Wrapf(err, "susb: open year %d", year)
		}
		rows, err := d.parseCSV(ctx, pool, file, year)
		_ = file.Close()
		if err != nil {
			return nil, eris.Wrapf(err, "susb: process year %d", year)
		}

		totalRows += rows
		log.Info("processed SUSB year", zap.Int("year", year), zap.Int64("rows", rows))

		_ = os.Remove(txtPath)
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"start_year": susbStartYear, "end_year": currentYear},
	}, nil
}

func (d *SUSB) parseCSV(ctx context.Context, pool db.Pool, r io.Reader, year int) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "susb: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := []string{"year", "fips_state", "naics", "entrsizedscr", "firm", "estb", "empl", "payr"}
	conflictKeys := []string{"year", "fips_state", "naics", "entrsizedscr"}

	var batch [][]any
	var totalRows int64
	seen := make(map[string]int) // conflict key → batch index (dedup within batch)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		naics := trimQuotes(getCol(record, colIdx, "naics"))
		if !transform.IsRelevantNAICS(naics) {
			continue
		}
		naics = transform.NormalizeNAICS(naics)

		fipsState := transform.NormalizeFIPSState(trimQuotes(getCol(record, colIdx, "state")))
		entrSize := trimQuotes(getCol(record, colIdx, "entrsizedscr"))

		row := []any{
			int16(year),
			fipsState,
			naics,
			entrSize,
			parseIntOr(trimQuotes(getCol(record, colIdx, "firm")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "estb")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "empl")), 0),
			parseInt64Or(trimQuotes(getCol(record, colIdx, "payr")), 0),
		}

		// Deduplicate by conflict key within the batch to avoid
		// "ON CONFLICT DO UPDATE cannot affect row a second time".
		key := fmt.Sprintf("%d|%s|%s|%s", year, fipsState, naics, entrSize)
		if idx, exists := seen[key]; exists {
			batch[idx] = row // overwrite with latest
			continue
		}
		seen[key] = len(batch)
		batch = append(batch, row)

		if len(batch) >= susbBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.susb_data",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrap(err, "susb: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
			seen = make(map[string]int)
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.susb_data",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrap(err, "susb: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}
