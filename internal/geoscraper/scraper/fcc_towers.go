package scraper

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

// FCCTowers scrapes cellular tower locations from the HIFLD ArcGIS shapefile.
type FCCTowers struct {
	downloadURL string // override for testing; empty uses default
}

// Name implements GeoScraper.
func (f *FCCTowers) Name() string { return "fcc_towers" }

// Table implements GeoScraper.
func (f *FCCTowers) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (f *FCCTowers) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (f *FCCTowers) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (f *FCCTowers) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.March)
}

// Sync implements GeoScraper.
func (f *FCCTowers) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", f.Name()))
	log.Info("starting FCC towers sync")

	url := fccTowerURL
	if f.downloadURL != "" {
		url = f.downloadURL
	}

	// Download shapefile ZIP.
	zipPath := filepath.Join(tempDir, "fcc_towers.zip")
	if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "fcc_towers: download shapefile")
	}

	// Extract ZIP.
	extractDir := filepath.Join(tempDir, "fcc_towers")
	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "fcc_towers: extract ZIP")
	}

	// Find .shp file.
	var shpPath string
	for _, fp := range files {
		if strings.HasSuffix(strings.ToLower(fp), ".shp") {
			shpPath = fp
			break
		}
	}
	if shpPath == "" {
		return nil, eris.New("fcc_towers: no .shp file found in archive")
	}

	// Parse shapefile.
	rows, err := tiger.ParseShapefile(shpPath, fccTowerProduct)
	if err != nil {
		return nil, eris.Wrap(err, "fcc_towers: parse shapefile")
	}

	// Batch and upsert.
	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        f.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "fcc_towers: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, shpRow := range rows {
		row, ok := newTowerRow(shpRow)
		if !ok {
			continue
		}
		batch = append(batch, row)
		if len(batch) >= fccBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("FCC towers sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
