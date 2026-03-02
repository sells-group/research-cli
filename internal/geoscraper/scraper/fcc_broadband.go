package scraper

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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

// FCCBroadband scrapes broadband coverage data from the FCC Broadband Data Collection.
type FCCBroadband struct {
	downloadURL string // override for testing; empty uses default
	apiKey      string // FCC BDC API key
}

// Name implements GeoScraper.
func (f *FCCBroadband) Name() string { return "fcc_broadband" }

// Table implements GeoScraper.
func (f *FCCBroadband) Table() string { return "geo.broadband_coverage" }

// Category implements GeoScraper.
func (f *FCCBroadband) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (f *FCCBroadband) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (f *FCCBroadband) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.July)
}

// Sync implements GeoScraper.
func (f *FCCBroadband) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	if f.apiKey == "" {
		return nil, eris.New("fcc_broadband: FCC BDC API key required (fedsync.fcc_bdc_key)")
	}

	log := zap.L().With(zap.String("scraper", f.Name()))
	log.Info("starting FCC broadband sync")

	url := f.downloadURL
	if url == "" {
		return nil, eris.New("fcc_broadband: download URL is required")
	}

	// Download CSV ZIP.
	zipPath := filepath.Join(tempDir, "fcc_broadband.zip")
	if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "fcc_broadband: download CSV")
	}

	// Extract ZIP.
	extractDir := filepath.Join(tempDir, "fcc_broadband")
	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "fcc_broadband: extract ZIP")
	}

	// Find CSV files.
	var csvPaths []string
	for _, fp := range files {
		if strings.HasSuffix(strings.ToLower(fp), ".csv") {
			csvPaths = append(csvPaths, fp)
		}
	}
	if len(csvPaths) == 0 {
		return nil, eris.New("fcc_broadband: no CSV files found in archive")
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        f.Table(),
			Columns:      broadbandCols,
			ConflictKeys: broadbandConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "fcc_broadband: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, csvPath := range csvPaths {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if err := f.processCSV(csvPath, &batch, flush); err != nil {
			return nil, err
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("FCC broadband sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// processCSV reads a single BDC CSV file and appends rows to the batch.
func (f *FCCBroadband) processCSV(csvPath string, batch *[][]any, flush func() error) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return eris.Wrapf(err, "fcc_broadband: open CSV %s", filepath.Base(csvPath))
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)

	// Read header to build column index.
	header, err := reader.Read()
	if err != nil {
		return eris.Wrap(err, "fcc_broadband: read CSV header")
	}
	colIdx := make(map[string]int, len(header))
	for i, h := range header {
		colIdx[strings.TrimSpace(strings.ToLower(h))] = i
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return eris.Wrap(err, "fcc_broadband: read CSV row")
		}

		row, ok := parseBroadbandRow(record, colIdx)
		if !ok {
			continue
		}
		*batch = append(*batch, row)

		if len(*batch) >= fccBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

// parseBroadbandRow builds a row for geo.broadband_coverage from a CSV record.
// Returns nil, false if required fields are missing.
func parseBroadbandRow(record []string, colIdx map[string]int) ([]any, bool) {
	blockGEOID := csvField(record, colIdx, "block_geoid")
	if blockGEOID == "" {
		return nil, false
	}

	techCode := csvField(record, colIdx, "technology")
	tech := fccTechName(techCode)

	maxDown := csvFloat(record, colIdx, "max_download_speed")
	maxUp := csvFloat(record, colIdx, "max_upload_speed")
	providers := csvInt(record, colIdx, "provider_count")
	lat := csvFloat(record, colIdx, "latitude")
	lon := csvFloat(record, colIdx, "longitude")

	sourceID := fmt.Sprintf("fcc_bdc/%s/%s", blockGEOID, tech)
	props, _ := json.Marshal(map[string]any{
		"technology_code": techCode,
	})

	return []any{
		blockGEOID,
		tech,
		maxDown,
		maxUp,
		providers,
		lat,
		lon,
		fccSource,
		sourceID,
		props,
	}, true
}

// csvField safely extracts a string field from a CSV record.
func csvField(record []string, colIdx map[string]int, key string) string {
	idx, ok := colIdx[key]
	if !ok || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

// csvFloat safely extracts a float64 field from a CSV record.
func csvFloat(record []string, colIdx map[string]int, key string) float64 {
	s := csvField(record, colIdx, key)
	if s == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// csvInt safely extracts an int field from a CSV record.
func csvInt(record []string, colIdx map[string]int, key string) int {
	s := csvField(record, colIdx, key)
	if s == "" {
		return 0
	}
	n, _ := strconv.Atoi(s)
	return n
}
