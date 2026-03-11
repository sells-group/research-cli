package scraper

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// fraRRCrossingExclude lists CSV columns stored in dedicated DB columns.
var fraRRCrossingExclude = map[string]bool{
	"CROSSING":  true,
	"STREET":    true,
	"TYPEXING":  true,
	"LATITUDE":  true,
	"LONGITUDE": true,
}

const fraRRCrossingsDefaultURL = "https://safetydata.fra.dot.gov/OfficeofSafety/publicsite/downloads/CrossingInventoryData.zip"

// FRARRCrossings scrapes railroad crossing data from FRA bulk CSV downloads.
type FRARRCrossings struct {
	baseURL string // override for testing; empty uses default FRA endpoint
}

// Name implements GeoScraper.
func (s *FRARRCrossings) Name() string { return "fra_rr_crossings" }

// Table implements GeoScraper.
func (s *FRARRCrossings) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *FRARRCrossings) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *FRARRCrossings) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (s *FRARRCrossings) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.QuarterlyAfterDelay(now, lastSync, 0)
}

// Sync implements GeoScraper.
func (s *FRARRCrossings) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting fra_rr_crossings sync")

	url := s.baseURL
	if url == "" {
		url = fraRRCrossingsDefaultURL
	}

	zipPath := filepath.Join(tempDir, "crossings.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "fra_rr_crossings: download")
	}

	csvPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "fra_rr_crossings: extract zip")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "fra_rr_crossings: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "fra_rr_crossings: read header")
	}
	cols := csvColIndex(header)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "fra_rr_crossings: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for {
		row, rErr := reader.Read()
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return nil, eris.Wrap(rErr, "fra_rr_crossings: read row")
		}

		lat := csvFloat64(row, cols["LATITUDE"])
		lon := csvFloat64(row, cols["LONGITUDE"])
		if lat == 0 && lon == 0 {
			continue
		}

		sourceID := csvString(row, cols["CROSSING"])
		if sourceID == "" {
			continue
		}

		batch = append(batch, []any{
			csvString(row, cols["STREET"]),
			"rr_crossing",
			csvString(row, cols["TYPEXING"]),
			0.0,
			lat,
			lon,
			"fra",
			sourceID,
			csvProperties(row, header, fraRRCrossingExclude),
		})

		if len(batch) >= hifldBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("fra_rr_crossings sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
