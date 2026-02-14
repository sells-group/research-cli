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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	oewsStartYear = 2019
	oewsBatchSize  = 5000
)

// OEWS implements the BLS Occupational Employment and Wage Statistics dataset.
type OEWS struct{}

func (d *OEWS) Name() string    { return "oews" }
func (d *OEWS) Table() string   { return "fed_data.oews_data" }
func (d *OEWS) Phase() Phase    { return Phase1 }
func (d *OEWS) Cadence() Cadence { return Annual }

func (d *OEWS) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.April)
}

func (d *OEWS) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "oews"))
	var totalRows int64

	currentYear := time.Now().Year() - 1

	for year := oewsStartYear; year <= currentYear; year++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		yy := fmt.Sprintf("%02d", year%100)
		url := fmt.Sprintf("https://www.bls.gov/oes/special-requests/oesm%snat.zip", yy)
		log.Info("downloading OEWS data", zap.Int("year", year), zap.String("url", url))

		zipPath := filepath.Join(tempDir, fmt.Sprintf("oews_%d.zip", year))
		if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
			return nil, eris.Wrapf(err, "oews: download year %d", year)
		}

		rows, err := d.processZip(ctx, pool, zipPath, year)
		if err != nil {
			return nil, eris.Wrapf(err, "oews: process year %d", year)
		}

		totalRows += rows
		log.Info("processed OEWS year", zap.Int("year", year), zap.Int64("rows", rows))

		_ = os.Remove(zipPath)
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"start_year": oewsStartYear, "end_year": currentYear},
	}, nil
}

func (d *OEWS) processZip(ctx context.Context, pool *pgxpool.Pool, zipPath string, year int) (int64, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, eris.Wrap(err, "oews: open zip")
	}
	defer zr.Close()

	for _, zf := range zr.File {
		name := strings.ToLower(zf.Name)
		// Look for the national-level all-data CSV
		if strings.Contains(name, "nat") && (strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".txt")) {
			rc, err := zf.Open()
			if err != nil {
				return 0, eris.Wrapf(err, "oews: open file %s", zf.Name)
			}
			n, err := d.parseCSV(ctx, pool, rc, year)
			rc.Close()
			return n, err
		}
	}

	// Fallback: try first CSV file
	for _, zf := range zr.File {
		name := strings.ToLower(zf.Name)
		if strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".txt") {
			rc, err := zf.Open()
			if err != nil {
				return 0, eris.Wrapf(err, "oews: open file %s", zf.Name)
			}
			n, err := d.parseCSV(ctx, pool, rc, year)
			rc.Close()
			return n, err
		}
	}

	return 0, eris.New("oews: no CSV found in zip")
}

func (d *OEWS) parseCSV(ctx context.Context, pool *pgxpool.Pool, r io.Reader, year int) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "oews: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := []string{"area_code", "area_type", "naics", "occ_code", "year", "tot_emp", "h_mean", "a_mean", "h_median", "a_median"}
	conflictKeys := []string{"area_code", "naics", "occ_code", "year"}

	var batch [][]any
	var totalRows int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		naics := trimQuotes(getCol(record, colIdx, "naics"))
		// Also check "naics_title" for context but filter on code
		if naics == "" {
			naics = trimQuotes(getCol(record, colIdx, "i_group"))
		}
		if !transform.IsRelevantNAICS(naics) {
			continue
		}

		areaCode := trimQuotes(getCol(record, colIdx, "area"))
		if areaCode == "" {
			areaCode = trimQuotes(getCol(record, colIdx, "area_code"))
		}
		areaType := parseInt16Or(trimQuotes(getCol(record, colIdx, "area_type")), 0)
		occCode := trimQuotes(getCol(record, colIdx, "occ_code"))

		row := []any{
			areaCode,
			areaType,
			naics,
			occCode,
			int16(year),
			parseIntOr(trimQuotes(getCol(record, colIdx, "tot_emp")), 0),
			parseFloat64Or(trimQuotes(getCol(record, colIdx, "h_mean")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "a_mean")), 0),
			parseFloat64Or(trimQuotes(getCol(record, colIdx, "h_median")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "a_median")), 0),
		}

		batch = append(batch, row)

		if len(batch) >= oewsBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.oews_data",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrap(err, "oews: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.oews_data",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrap(err, "oews: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}
