package scraper

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// lihtcExclude lists CSV columns stored in dedicated DB columns.
var lihtcExclude = map[string]bool{
	"HUD_ID":    true,
	"PROJECT":   true,
	"PROJ_ST":   true,
	"PROJ_ZIP":  true,
	"LATITUDE":  true,
	"LONGITUDE": true,
	"N_UNITS":   true,
	"LI_UNITS":  true,
	"YR_PIS":    true,
}

var lihtcCols = []string{
	"project_id", "project_name", "project_state", "project_zip",
	"latitude", "longitude", "total_units", "li_units", "year_placed",
	"source", "source_id", "properties",
}

var lihtcConflictKeys = []string{"source", "source_id"}

// HUDLihtc scrapes Low-Income Housing Tax Credit project data from HUD.
type HUDLihtc struct {
	baseURL string // override for testing; empty uses default HUD endpoint
}

// Name implements GeoScraper.
func (s *HUDLihtc) Name() string { return "hud_lihtc" }

// Table implements GeoScraper.
func (s *HUDLihtc) Table() string { return "geo.lihtc_projects" }

// Category implements GeoScraper.
func (s *HUDLihtc) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *HUDLihtc) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *HUDLihtc) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *HUDLihtc) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting hud_lihtc sync")

	url := s.baseURL
	if url == "" {
		url = "https://www.huduser.gov/lihtc/lihtcpub.zip"
	}

	zipPath := filepath.Join(tempDir, "lihtcpub.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "hud_lihtc: download")
	}

	extracted, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "hud_lihtc: extract zip")
	}

	// Find the CSV file in the extracted contents.
	var csvPath string
	for _, p := range extracted {
		if filepath.Ext(p) == ".csv" || filepath.Ext(p) == ".CSV" {
			csvPath = p
			break
		}
	}
	if csvPath == "" {
		return nil, eris.New("hud_lihtc: no CSV found in ZIP")
	}

	file, err := os.Open(csvPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrap(err, "hud_lihtc: open csv")
	}
	defer file.Close() //nolint:errcheck

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "hud_lihtc: read header")
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
			Columns:      lihtcCols,
			ConflictKeys: lihtcConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "hud_lihtc: upsert batch")
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
			return nil, eris.Wrap(rErr, "hud_lihtc: read row")
		}

		sourceID := csvString(row, cols["HUD_ID"])
		if sourceID == "" {
			continue
		}

		lat := csvFloat64(row, cols["LATITUDE"])
		lon := csvFloat64(row, cols["LONGITUDE"])

		yrPIS := csvString(row, cols["YR_PIS"])
		yearPlaced, _ := strconv.Atoi(yrPIS)

		nUnits := csvString(row, cols["N_UNITS"])
		totalUnits, _ := strconv.Atoi(nUnits)

		liUnits := csvString(row, cols["LI_UNITS"])
		lowIncUnits, _ := strconv.Atoi(liUnits)

		batch = append(batch, []any{
			sourceID,
			csvString(row, cols["PROJECT"]),
			csvString(row, cols["PROJ_ST"]),
			csvString(row, cols["PROJ_ZIP"]),
			lat,
			lon,
			totalUnits,
			lowIncUnits,
			yearPlaced,
			"hud_lihtc",
			sourceID,
			csvProperties(row, header, lihtcExclude),
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

	log.Info("hud_lihtc sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
