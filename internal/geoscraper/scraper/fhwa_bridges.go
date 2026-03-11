package scraper

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// fhwaBridgeExclude lists CSV columns stored in dedicated DB columns.
var fhwaBridgeExclude = map[string]bool{
	"STRUCTURE_NUMBER_008": true,
	"FACILITY_CARRIED_007": true,
	"YEAR_BUILT_027":       true,
	"DECK_AREA":            true,
	"LAT_016":              true,
	"LONG_017":             true,
}

// FHWABridges scrapes NBI bridge data from FHWA bulk CSV downloads.
type FHWABridges struct {
	baseURL string // override for testing; empty uses default FHWA endpoint
}

// Name implements GeoScraper.
func (s *FHWABridges) Name() string { return "fhwa_bridges" }

// Table implements GeoScraper.
func (s *FHWABridges) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *FHWABridges) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *FHWABridges) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *FHWABridges) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// nbiDMS converts NBI degree-minute-second format (DDMMSS.ss) to decimal degrees.
func nbiDMS(raw string) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	// NBI format: DDMMSS.ss stored as a number
	sign := 1.0
	if val < 0 {
		sign = -1.0
		val = math.Abs(val)
	}
	deg := math.Floor(val / 1_000_000)
	mins := math.Floor(math.Mod(val, 1_000_000) / 10_000)
	sec := math.Mod(val, 10_000) / 100
	return sign * (deg + mins/60 + sec/3600)
}

// Sync implements GeoScraper.
func (s *FHWABridges) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting fhwa_bridges sync")

	url := s.baseURL
	if url == "" {
		year := time.Now().Year()
		url = fmt.Sprintf("https://www.fhwa.dot.gov/bridge/nbi/ascii%d.zip", year)
	}

	zipPath := filepath.Join(tempDir, "nbi.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "fhwa_bridges: download")
	}

	csvPath, err := fetcher.ExtractZIPSingle(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "fhwa_bridges: extract zip")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "fhwa_bridges: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "fhwa_bridges: read header")
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
			return eris.Wrap(uErr, "fhwa_bridges: upsert batch")
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
			return nil, eris.Wrap(rErr, "fhwa_bridges: read row")
		}

		lat := nbiDMS(csvString(row, cols["LAT_016"]))
		lon := -nbiDMS(csvString(row, cols["LONG_017"])) // NBI stores west longitude as positive
		if lat == 0 && lon == 0 {
			continue
		}

		sourceID := csvString(row, cols["STRUCTURE_NUMBER_008"])
		if sourceID == "" {
			continue
		}

		batch = append(batch, []any{
			csvString(row, cols["FACILITY_CARRIED_007"]),
			"bridge",
			csvString(row, cols["YEAR_BUILT_027"]),
			csvFloat64(row, cols["DECK_AREA"]),
			lat,
			lon,
			"fhwa",
			sourceID,
			csvProperties(row, header, fhwaBridgeExclude),
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

	log.Info("fhwa_bridges sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
