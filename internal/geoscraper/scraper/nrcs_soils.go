package scraper

import (
	"context"
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

// NRCSSoils scrapes soil map unit polygons from the NRCS gSSURGO national
// soils shapefile.
type NRCSSoils struct {
	downloadURL string // override for testing; empty uses default NRCS endpoint
}

// Name implements GeoScraper.
func (n *NRCSSoils) Name() string { return "nrcs_soils" }

// Table implements GeoScraper.
func (n *NRCSSoils) Table() string { return "geo.soils" }

// Category implements GeoScraper.
func (n *NRCSSoils) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (n *NRCSSoils) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (n *NRCSSoils) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (n *NRCSSoils) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", n.Name()))
	log.Info("starting NRCS soils sync")

	url := nrcsURL(n.downloadURL)
	zipPath := filepath.Join(tempDir, "nrcs_soils.zip")

	log.Debug("downloading NRCS soils shapefile")
	if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "nrcs_soils: download shapefile")
	}

	extractDir := filepath.Join(tempDir, "nrcs_soils")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return nil, eris.Wrap(err, "nrcs_soils: create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrap(err, "nrcs_soils: extract ZIP")
	}

	shpPath, err := findShpFile(extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "nrcs_soils: find .shp file")
	}

	rows, err := tiger.ParseShapefile(shpPath, nrcsProduct)
	if err != nil {
		return nil, eris.Wrap(err, "nrcs_soils: parse shapefile")
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        n.Table(),
			Columns:      soilCols,
			ConflictKeys: soilConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "nrcs_soils: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, shpRow := range rows {
		row, ok := newSoilRow(shpRow)
		if !ok {
			continue
		}
		batch = append(batch, row)
		if len(batch) >= nrcsBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("NRCS soils sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
