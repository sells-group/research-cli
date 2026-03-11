package scraper

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// faaAirportExclude lists CSV columns stored in dedicated DB columns.
var faaAirportExclude = map[string]bool{
	"ARPT_ID":        true,
	"ARPT_NAME":      true,
	"SITE_TYPE_CODE": true,
	"LAT_DECIMAL":    true,
	"LONG_DECIMAL":   true,
}

// FAAAirports scrapes airport data from FAA NASR 28-day subscription CSV.
type FAAAirports struct {
	baseURL string // override for testing; empty uses default FAA endpoint
}

// Name implements GeoScraper.
func (s *FAAAirports) Name() string { return "faa_airports" }

// Table implements GeoScraper.
func (s *FAAAirports) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *FAAAirports) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *FAAAirports) Cadence() geoscraper.Cadence { return geoscraper.Monthly }

// ShouldRun implements GeoScraper.
func (s *FAAAirports) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.MonthlySchedule(now, lastSync)
}

// findAPTBase searches extracted files for the APT_BASE.csv file.
func findAPTBase(files []string) string {
	for _, f := range files {
		if strings.HasSuffix(strings.ToUpper(filepath.Base(f)), "APT_BASE.CSV") {
			return f
		}
	}
	return ""
}

// Sync implements GeoScraper.
func (s *FAAAirports) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting faa_airports sync")

	url := s.baseURL
	if url == "" {
		// FAA publishes 28-day NASR subscriptions; use current date to find the right one.
		// The exact URL changes per cycle. We'll use the known static path.
		url = "https://nfdc.faa.gov/webContent/28DaySub/extra/28DaySubscription_Effective_" +
			time.Now().Format("2006-01-02") + ".zip"
	}

	zipPath := filepath.Join(tempDir, "nasr.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "faa_airports: download")
	}

	files, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "faa_airports: extract zip")
	}

	csvPath := findAPTBase(files)
	if csvPath == "" {
		return nil, eris.New("faa_airports: APT_BASE.csv not found in ZIP")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "faa_airports: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "faa_airports: read header")
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
			return eris.Wrap(uErr, "faa_airports: upsert batch")
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
			return nil, eris.Wrap(rErr, "faa_airports: read row")
		}

		lat := csvFloat64(row, cols["LAT_DECIMAL"])
		lon := csvFloat64(row, cols["LONG_DECIMAL"])
		if lat == 0 && lon == 0 {
			continue
		}

		sourceID := csvString(row, cols["ARPT_ID"])
		if sourceID == "" {
			continue
		}

		batch = append(batch, []any{
			csvString(row, cols["ARPT_NAME"]),
			"airport",
			csvString(row, cols["SITE_TYPE_CODE"]),
			0.0,
			lat,
			lon,
			"faa",
			sourceID,
			csvProperties(row, header, faaAirportExclude),
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

	log.Info("faa_airports sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
