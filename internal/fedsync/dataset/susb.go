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
	susbStartYear = 2019
	susbBatchSize = 5000
)

// SUSB implements the Census Statistics of US Businesses dataset.
type SUSB struct{}

func (d *SUSB) Name() string    { return "susb" }
func (d *SUSB) Table() string   { return "fed_data.susb_data" }
func (d *SUSB) Phase() Phase    { return Phase1 }
func (d *SUSB) Cadence() Cadence { return Annual }

func (d *SUSB) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.March)
}

func (d *SUSB) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "susb"))
	var totalRows int64

	currentYear := time.Now().Year() - 1

	for year := susbStartYear; year <= currentYear; year++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := fmt.Sprintf("https://www2.census.gov/programs-surveys/susb/datasets/%d/us_state_totals_naics_%d.zip", year, year)
		log.Info("downloading SUSB data", zap.Int("year", year), zap.String("url", url))

		zipPath := filepath.Join(tempDir, fmt.Sprintf("susb_%d.zip", year))
		if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
			return nil, eris.Wrapf(err, "susb: download year %d", year)
		}

		rows, err := d.processZip(ctx, pool, zipPath, year)
		if err != nil {
			return nil, eris.Wrapf(err, "susb: process year %d", year)
		}

		totalRows += rows
		log.Info("processed SUSB year", zap.Int("year", year), zap.Int64("rows", rows))

		_ = os.Remove(zipPath)
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"start_year": susbStartYear, "end_year": currentYear},
	}, nil
}

func (d *SUSB) processZip(ctx context.Context, pool *pgxpool.Pool, zipPath string, year int) (int64, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, eris.Wrap(err, "susb: open zip")
	}
	defer zr.Close()

	for _, zf := range zr.File {
		name := strings.ToLower(zf.Name)
		if strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".txt") {
			rc, err := zf.Open()
			if err != nil {
				return 0, eris.Wrapf(err, "susb: open file %s in zip", zf.Name)
			}
			n, err := d.parseCSV(ctx, pool, rc, year)
			rc.Close()
			return n, err
		}
	}

	return 0, eris.New("susb: no CSV found in zip")
}

func (d *SUSB) parseCSV(ctx context.Context, pool *pgxpool.Pool, r io.Reader, year int) (int64, error) {
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

		fipsState := transform.NormalizeFIPSState(trimQuotes(getCol(record, colIdx, "statefips")))
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
