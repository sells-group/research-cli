package scraper

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
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// afdcExclude lists CSV columns stored in dedicated DB columns.
var afdcExclude = map[string]bool{
	"id":                 true,
	"Station Name":       true,
	"EV Connector Types": true,
	"EV Level2 EVSE Num": true,
	"EV DC Fast Count":   true,
	"Latitude":           true,
	"Longitude":          true,
}

// AFDCEVCharging scrapes EV charging station data from the DOE AFDC CSV API.
type AFDCEVCharging struct {
	baseURL string // override for testing; empty uses default AFDC endpoint
	apiKey  string // NREL API key; empty uses DEMO_KEY
}

// Name implements GeoScraper.
func (s *AFDCEVCharging) Name() string { return "afdc_ev_charging" }

// Table implements GeoScraper.
func (s *AFDCEVCharging) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *AFDCEVCharging) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *AFDCEVCharging) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (s *AFDCEVCharging) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.QuarterlyAfterDelay(now, lastSync, 0)
}

// evCapacity sums Level 2 and DC fast charger counts.
func evCapacity(row []string, cols map[string]int) float64 {
	return csvFloat64(row, cols["EV Level2 EVSE Num"]) + csvFloat64(row, cols["EV DC Fast Count"])
}

// Sync implements GeoScraper.
func (s *AFDCEVCharging) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting afdc_ev_charging sync")

	url := s.baseURL
	if url == "" {
		key := s.apiKey
		if key == "" {
			key = "DEMO_KEY"
		}
		url = fmt.Sprintf("https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?api_key=%s&fuel_type=ELEC", key)
	}

	csvPath := filepath.Join(tempDir, "afdc_ev.csv")
	if _, err := f.DownloadToFile(ctx, url, csvPath); err != nil {
		return nil, eris.Wrap(err, "afdc_ev_charging: download")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "afdc_ev_charging: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "afdc_ev_charging: read header")
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
			return eris.Wrap(uErr, "afdc_ev_charging: upsert batch")
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
			return nil, eris.Wrap(rErr, "afdc_ev_charging: read row")
		}

		lat := csvFloat64(row, cols["Latitude"])
		lon := csvFloat64(row, cols["Longitude"])
		if lat == 0 && lon == 0 {
			continue
		}

		sourceID := csvString(row, cols["id"])
		if sourceID == "" {
			continue
		}

		connectors := csvString(row, cols["EV Connector Types"])
		connectors = strings.ReplaceAll(connectors, "\n", ", ")

		batch = append(batch, []any{
			csvString(row, cols["Station Name"]),
			"ev_charging",
			connectors,
			evCapacity(row, cols),
			lat,
			lon,
			"afdc",
			sourceID,
			csvProperties(row, header, afdcExclude),
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

	log.Info("afdc_ev_charging sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
