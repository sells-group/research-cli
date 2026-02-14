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

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	qcewStartYear = 2019
	qcewBatchSize = 10000
	qcewLagMonths = 5
)

// QCEW implements the BLS Quarterly Census of Employment and Wages dataset.
type QCEW struct{}

func (d *QCEW) Name() string     { return "qcew" }
func (d *QCEW) Table() string    { return "fed_data.qcew_data" }
func (d *QCEW) Phase() Phase     { return Phase1 }
func (d *QCEW) Cadence() Cadence { return Quarterly }

func (d *QCEW) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, qcewLagMonths)
}

func (d *QCEW) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "qcew"))
	var totalRows int64

	currentYear := time.Now().Year() - 1

	for year := qcewStartYear; year <= currentYear; year++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := fmt.Sprintf("https://data.bls.gov/cew/data/files/%d/csv/%d_qtrly_by_industry.zip", year, year)
		log.Info("downloading QCEW data", zap.Int("year", year), zap.String("url", url))

		zipPath := filepath.Join(tempDir, fmt.Sprintf("qcew_%d.zip", year))
		if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
			return nil, eris.Wrapf(err, "qcew: download year %d", year)
		}

		rows, err := d.processZip(ctx, pool, zipPath, year)
		if err != nil {
			return nil, eris.Wrapf(err, "qcew: process year %d", year)
		}

		totalRows += rows
		log.Info("processed QCEW year", zap.Int("year", year), zap.Int64("rows", rows))

		_ = os.Remove(zipPath)
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"start_year": qcewStartYear, "end_year": currentYear},
	}, nil
}

func (d *QCEW) processZip(ctx context.Context, pool db.Pool, zipPath string, year int) (int64, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, eris.Wrap(err, "qcew: open zip")
	}
	defer zr.Close()

	var totalRows int64

	for _, zf := range zr.File {
		name := strings.ToLower(zf.Name)
		// QCEW ZIP contains one CSV per industry; filter to relevant sectors.
		if !strings.HasSuffix(name, ".csv") {
			continue
		}
		if !d.isRelevantFile(name) {
			continue
		}

		rc, err := zf.Open()
		if err != nil {
			return totalRows, eris.Wrapf(err, "qcew: open file %s", zf.Name)
		}
		n, err := d.parseCSV(ctx, pool, rc, year)
		rc.Close()
		if err != nil {
			return totalRows, eris.Wrapf(err, "qcew: parse file %s", zf.Name)
		}
		totalRows += n
	}

	return totalRows, nil
}

// isRelevantFile checks if a QCEW CSV file is for a relevant industry sector.
// Files are named like "2023.q1-q4.by_industry/2023.q1-q4 52 NAICS 52.csv".
func (d *QCEW) isRelevantFile(name string) bool {
	for _, prefix := range transform.NAICSPrefixes {
		if strings.Contains(name, " "+prefix+" ") || strings.Contains(name, " "+prefix+".") {
			return true
		}
	}
	// Also accept aggregate/total files
	if strings.Contains(name, "10 total") {
		return true
	}
	return false
}

func (d *QCEW) parseCSV(ctx context.Context, pool db.Pool, r io.Reader, year int) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "qcew: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := []string{"area_fips", "own_code", "industry_code", "year", "qtr", "month1_emplvl", "month2_emplvl", "month3_emplvl", "total_qtrly_wages", "avg_wkly_wage", "qtrly_estabs"}
	conflictKeys := []string{"area_fips", "own_code", "industry_code", "year", "qtr"}

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

		industryCode := trimQuotes(getCol(record, colIdx, "industry_code"))
		if !transform.IsRelevantNAICS(industryCode) {
			continue
		}

		areaFips := trimQuotes(getCol(record, colIdx, "area_fips"))
		ownCode := trimQuotes(getCol(record, colIdx, "own_code"))
		qtr := parseInt16Or(trimQuotes(getCol(record, colIdx, "qtr")), 0)

		// Skip annual aggregates (qtr=0)
		if qtr == 0 {
			continue
		}

		row := []any{
			areaFips,
			ownCode,
			industryCode,
			int16(year),
			qtr,
			parseIntOr(trimQuotes(getCol(record, colIdx, "month1_emplvl")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "month2_emplvl")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "month3_emplvl")), 0),
			parseInt64Or(trimQuotes(getCol(record, colIdx, "total_qtrly_wages")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "avg_wkly_wage")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "qtrly_estabs")), 0),
		}

		batch = append(batch, row)

		if len(batch) >= qcewBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.qcew_data",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrap(err, "qcew: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.qcew_data",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrap(err, "qcew: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}
