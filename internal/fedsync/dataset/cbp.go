package dataset

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	cbpStartYear = 2019
	cbpBatchSize = 5000
)

// CBP implements the Census County Business Patterns dataset.
type CBP struct{}

// Name implements Dataset.
func (d *CBP) Name() string { return "cbp" }

// Table implements Dataset.
func (d *CBP) Table() string { return "fed_data.cbp_data" }

// Phase implements Dataset.
func (d *CBP) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *CBP) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *CBP) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

// Sync fetches and loads Census County Business Patterns data.
func (d *CBP) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "cbp"))
	var totalRows atomic.Int64

	currentYear := time.Now().Year() - 1 // CBP data lags by ~1 year

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(3)

	for year := cbpStartYear; year <= currentYear; year++ {
		// Download county-level file.
		g.Go(func() error {
			yy := fmt.Sprintf("%02d", year%100)
			url := fmt.Sprintf("https://www2.census.gov/programs-surveys/cbp/datasets/%d/cbp%sco.zip", year, yy)

			log.Info("downloading CBP county data", zap.Int("year", year), zap.String("url", url))

			zipPath := filepath.Join(tempDir, fmt.Sprintf("cbp%sco.zip", yy))
			if _, err := f.DownloadToFile(gctx, url, zipPath); err != nil {
				if strings.Contains(err.Error(), "status 404") {
					log.Info("CBP county data not yet available, skipping", zap.Int("year", year))
					return nil
				}
				return eris.Wrapf(err, "cbp: download county year %d", year)
			}

			rows, err := d.processZip(gctx, pool, zipPath, year)
			if err != nil {
				return eris.Wrapf(err, "cbp: process county year %d", year)
			}

			totalRows.Add(rows)
			log.Info("processed CBP county year", zap.Int("year", year), zap.Int64("rows", rows))

			_ = os.Remove(zipPath)
			return nil
		})
		// Download state-level file (fips_county='000', used by mv_market_size).
		g.Go(func() error {
			yy := fmt.Sprintf("%02d", year%100)
			url := fmt.Sprintf("https://www2.census.gov/programs-surveys/cbp/datasets/%d/cbp%sst.zip", year, yy)

			log.Info("downloading CBP state data", zap.Int("year", year), zap.String("url", url))

			zipPath := filepath.Join(tempDir, fmt.Sprintf("cbp%sst.zip", yy))
			if _, err := f.DownloadToFile(gctx, url, zipPath); err != nil {
				if strings.Contains(err.Error(), "status 404") {
					log.Info("CBP state data not yet available, skipping", zap.Int("year", year))
					return nil
				}
				return eris.Wrapf(err, "cbp: download state year %d", year)
			}

			rows, err := d.processZip(gctx, pool, zipPath, year)
			if err != nil {
				return eris.Wrapf(err, "cbp: process state year %d", year)
			}

			totalRows.Add(rows)
			log.Info("processed CBP state year", zap.Int("year", year), zap.Int64("rows", rows))

			_ = os.Remove(zipPath)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows.Load(),
		Metadata:   map[string]any{"start_year": cbpStartYear, "end_year": currentYear},
	}, nil
}

func (d *CBP) processZip(ctx context.Context, pool db.Pool, zipPath string, year int) (int64, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, eris.Wrap(err, "cbp: open zip")
	}
	defer zr.Close() //nolint:errcheck

	for _, zf := range zr.File {
		if strings.HasSuffix(strings.ToLower(zf.Name), ".csv") || strings.HasSuffix(strings.ToLower(zf.Name), ".txt") {
			rc, err := zf.Open()
			if err != nil {
				return 0, eris.Wrapf(err, "cbp: open file %s in zip", zf.Name)
			}
			n, err := d.parseCSV(ctx, pool, rc, year)
			_ = rc.Close()
			return n, err
		}
	}

	return 0, eris.New("cbp: no CSV found in zip")
}

func (d *CBP) parseCSV(ctx context.Context, pool db.Pool, r io.Reader, year int) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "cbp: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := []string{"year", "fips_state", "fips_county", "naics", "emp", "emp_nf", "qp1", "qp1_nf", "ap", "ap_nf", "est"}
	conflictKeys := []string{"year", "fips_state", "fips_county", "naics"}

	var batch [][]any
	var totalRows int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}

		// State-level files have an "lfo" (legal form of organization) column
		// with multiple rows per state+NAICS. Keep only the total row (lfo="-").
		if lfo := trimQuotes(getCol(record, colIdx, "lfo")); lfo != "" && lfo != "-" {
			continue
		}

		naics := trimQuotes(getCol(record, colIdx, "naics"))
		if !transform.IsRelevantNAICS(naics) {
			continue
		}
		naics = transform.NormalizeNAICS(naics)

		fipsState := transform.NormalizeFIPSState(trimQuotes(getCol(record, colIdx, "fipstate")))
		fipsCounty := transform.NormalizeFIPSCounty(trimQuotes(getCol(record, colIdx, "fipscty")))

		row := []any{
			int16(year),
			fipsState,
			fipsCounty,
			naics,
			parseIntOr(trimQuotes(getCol(record, colIdx, "emp")), 0),
			firstChar(trimQuotes(getCol(record, colIdx, "emp_nf"))),
			parseInt64Or(trimQuotes(getCol(record, colIdx, "qp1")), 0),
			firstChar(trimQuotes(getCol(record, colIdx, "qp1_nf"))),
			parseInt64Or(trimQuotes(getCol(record, colIdx, "ap")), 0),
			firstChar(trimQuotes(getCol(record, colIdx, "ap_nf"))),
			parseIntOr(trimQuotes(getCol(record, colIdx, "est")), 0),
		}

		batch = append(batch, row)

		if len(batch) >= cbpBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.cbp_data",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrap(err, "cbp: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.cbp_data",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrap(err, "cbp: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}

// mapColumns builds a case-insensitive column name to index map.
func mapColumns(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, col := range header {
		m[strings.ToLower(strings.TrimSpace(col))] = i
	}
	return m
}

// getCol gets a column value by name from a CSV record, returning empty string if not found.
func getCol(record []string, colIdx map[string]int, name string) string {
	idx, ok := colIdx[strings.ToLower(name)]
	if !ok || idx >= len(record) {
		return ""
	}
	return record[idx]
}
