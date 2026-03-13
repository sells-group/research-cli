package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

// PADUSProtectedAreasBulk scrapes protected area data from USGS PAD-US national GDB.
type PADUSProtectedAreasBulk struct {
	baseURL string // override for testing; empty uses default USGS ScienceBase URL
}

// Name implements GeoScraper.
func (s *PADUSProtectedAreasBulk) Name() string { return "padus_protected_areas_bulk" }

// Table implements GeoScraper.
func (s *PADUSProtectedAreasBulk) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *PADUSProtectedAreasBulk) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *PADUSProtectedAreasBulk) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *PADUSProtectedAreasBulk) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *PADUSProtectedAreasBulk) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting padus_protected_areas_bulk sync")

	url := s.baseURL
	if url == "" {
		url = "https://www.sciencebase.gov/catalog/file/get/652db2dfd34e44db0e2bdb28?f=__disk__PAD-US4_0Geodatabase.zip"
	}

	zipPath := filepath.Join(tempDir, "padus.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "padus: download")
	}

	extractDir := filepath.Join(tempDir, "padus")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "padus: create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrap(err, "padus: extract zip")
	}

	gdbPath := findGDB(extractDir)
	if gdbPath == "" {
		return nil, eris.New("padus: no GDB found in archive")
	}

	result, err := tiger.ParseGDB(ctx, gdbPath, "PADUS4_0Fee", extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "padus: parse GDB")
	}

	cols := make(map[string]int)
	for i, c := range result.Columns {
		cols[c] = i
	}

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
			return eris.Wrap(uErr, "padus: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for i, raw := range result.Rows {
		name := strValFromMap(raw, cols, "unit_nm")
		gapSts := strValFromMap(raw, cols, "gap_sts")
		gisAcres := strValFromMap(raw, cols, "gis_acres")
		sourceID := strValFromMap(raw, cols, "featclass")
		if sourceID == "" {
			sourceID = fmt.Sprintf("padus_%d", i)
		}

		props, _ := json.Marshal(map[string]any{
			"gap_status": gapSts,
			"gis_acres":  gisAcres,
			"state_nm":   strValFromMap(raw, cols, "state_nm"),
			"own_type":   strValFromMap(raw, cols, "own_type"),
			"mang_type":  strValFromMap(raw, cols, "mang_type"),
		})

		batch = append(batch, []any{
			name,
			"protected_area",
			gapSts,
			0.0,
			0.0, 0.0,
			"padus",
			sourceID,
			props,
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

	log.Info("padus_protected_areas_bulk sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
