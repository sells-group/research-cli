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

// usaceDamExclude lists CSV columns stored in dedicated DB columns.
var usaceDamExclude = map[string]bool{
	"NID_ID":     true,
	"DAM_NAME":   true,
	"DAM_TYPE":   true,
	"NID_HEIGHT": true,
	"LATITUDE":   true,
	"LONGITUDE":  true,
}

const usaceDamsDefaultURL = "https://nid.sec.usace.army.mil/api/nation/csv"

// USACEDams scrapes dam data from the USACE National Inventory of Dams CSV.
type USACEDams struct {
	baseURL string // override for testing; empty uses default USACE endpoint
}

// Name implements GeoScraper.
func (s *USACEDams) Name() string { return "usace_dams" }

// Table implements GeoScraper.
func (s *USACEDams) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *USACEDams) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *USACEDams) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *USACEDams) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *USACEDams) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting usace_dams sync")

	url := s.baseURL
	if url == "" {
		url = usaceDamsDefaultURL
	}

	csvPath := filepath.Join(tempDir, "nid.csv")
	if _, err := f.DownloadToFile(ctx, url, csvPath); err != nil {
		return nil, eris.Wrap(err, "usace_dams: download")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "usace_dams: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "usace_dams: read header")
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
			return eris.Wrap(uErr, "usace_dams: upsert batch")
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
			return nil, eris.Wrap(rErr, "usace_dams: read row")
		}

		lat := csvFloat64(row, cols["LATITUDE"])
		lon := csvFloat64(row, cols["LONGITUDE"])
		if lat == 0 && lon == 0 {
			continue
		}

		sourceID := csvString(row, cols["NID_ID"])
		if sourceID == "" {
			continue
		}

		batch = append(batch, []any{
			csvString(row, cols["DAM_NAME"]),
			"dam",
			csvString(row, cols["DAM_TYPE"]),
			csvFloat64(row, cols["NID_HEIGHT"]),
			lat,
			lon,
			"usace",
			sourceID,
			csvProperties(row, header, usaceDamExclude),
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

	log.Info("usace_dams sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
