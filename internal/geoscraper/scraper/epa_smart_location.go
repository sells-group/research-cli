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

// sldExclude lists CSV columns stored in dedicated DB columns.
var sldExclude = map[string]bool{
	"GEOID20":    true,
	"STATEFP":    true,
	"COUNTYFP":   true,
	"CBSA_Name":  true,
	"NatWalkInd": true,
	"D3B":        true,
	"D1C":        true,
	"D1A":        true,
	"TotEmp":     true,
	"AutoOwn0":   true,
}

var sldCols = []string{
	"geoid", "state_fips", "county_fips", "cbsa_name",
	"walkability_index", "transit_freq", "emp_density", "hh_density",
	"tot_emp", "auto_own_0_pct",
	"source", "source_id", "properties",
}

var sldConflictKeys = []string{"geoid"}

// EPASmartLocation scrapes the EPA Smart Location Database.
type EPASmartLocation struct {
	baseURL string // override for testing; empty uses default EPA endpoint
}

// Name implements GeoScraper.
func (s *EPASmartLocation) Name() string { return "epa_smart_location" }

// Table implements GeoScraper.
func (s *EPASmartLocation) Table() string { return "geo.smart_location" }

// Category implements GeoScraper.
func (s *EPASmartLocation) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *EPASmartLocation) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *EPASmartLocation) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *EPASmartLocation) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting epa_smart_location sync")

	url := s.baseURL
	if url == "" {
		url = "https://edg.epa.gov/EPADataCommons/public/OA/SLD/SmartLocationDatabaseV3.csv.zip"
	}

	zipPath := filepath.Join(tempDir, "sld.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "epa_smart_location: download")
	}

	files, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "epa_smart_location: extract zip")
	}

	csvPath := findSLDCSV(files)
	if csvPath == "" {
		return nil, eris.New("epa_smart_location: SLD CSV not found in ZIP")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "epa_smart_location: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "epa_smart_location: read header")
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
			Columns:      sldCols,
			ConflictKeys: sldConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "epa_smart_location: upsert batch")
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
			return nil, eris.Wrap(rErr, "epa_smart_location: read row")
		}

		geoid := csvString(row, cols["GEOID20"])
		if geoid == "" {
			continue
		}

		totEmp := int(csvFloat64(row, cols["TotEmp"]))

		batch = append(batch, []any{
			geoid,
			csvString(row, cols["STATEFP"]),
			csvString(row, cols["COUNTYFP"]),
			csvString(row, cols["CBSA_Name"]),
			csvFloat64(row, cols["NatWalkInd"]),
			csvFloat64(row, cols["D3B"]),
			csvFloat64(row, cols["D1C"]),
			csvFloat64(row, cols["D1A"]),
			totEmp,
			csvFloat64(row, cols["AutoOwn0"]),
			"epa_sld",
			geoid,
			csvProperties(row, header, sldExclude),
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

	log.Info("epa_smart_location sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// findSLDCSV finds the Smart Location Database CSV within extracted ZIP files.
func findSLDCSV(files []string) string {
	for _, f := range files {
		lower := strings.ToLower(filepath.Base(f))
		if strings.HasSuffix(lower, ".csv") && strings.Contains(lower, "smartlocation") {
			return f
		}
	}
	// Fallback: any CSV file.
	for _, f := range files {
		if strings.HasSuffix(strings.ToLower(f), ".csv") {
			return f
		}
	}
	return ""
}
