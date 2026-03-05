package scraper

import (
	"context"
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
	"github.com/sells-group/research-cli/internal/tiger"
)

// NWIWetlands scrapes wetland polygons from the US Fish & Wildlife Service
// National Wetlands Inventory shapefile downloads.
type NWIWetlands struct {
	downloadBaseURL string // override for testing; empty uses default FWS endpoint
}

// Name implements GeoScraper.
func (n *NWIWetlands) Name() string { return "nwi_wetlands" }

// Table implements GeoScraper.
func (n *NWIWetlands) Table() string { return "geo.wetlands" }

// Category implements GeoScraper.
func (n *NWIWetlands) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (n *NWIWetlands) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (n *NWIWetlands) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (n *NWIWetlands) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", n.Name()))
	log.Info("starting NWI wetlands sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		rows, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        n.Table(),
			Columns:      wetlandCols,
			ConflictKeys: wetlandConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "nwi_wetlands: upsert batch")
		}
		totalRows += rows
		batch = batch[:0]
		return nil
	}

	for _, state := range stateAbbrevs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := nwiDownloadURL(n.downloadBaseURL, state)
		zipPath := filepath.Join(tempDir, "nwi_"+state+".zip")

		log.Debug("downloading NWI shapefile", zap.String("state", state))

		if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
			log.Warn("nwi_wetlands: download failed, skipping state",
				zap.String("state", state), zap.Error(err))
			continue
		}

		extractDir := filepath.Join(tempDir, "nwi_"+state)
		if err := os.MkdirAll(extractDir, 0o750); err != nil {
			return nil, eris.Wrapf(err, "nwi_wetlands: create extract dir for %s", state)
		}

		if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
			log.Warn("nwi_wetlands: extract failed, skipping state",
				zap.String("state", state), zap.Error(err))
			continue
		}

		shpPath, err := findShpFile(extractDir)
		if err != nil {
			log.Warn("nwi_wetlands: no .shp file found, skipping state",
				zap.String("state", state), zap.Error(err))
			continue
		}

		result, err := tiger.ParseShapefile(shpPath, nwiProduct)
		if err != nil {
			log.Warn("nwi_wetlands: parse shapefile failed, skipping state",
				zap.String("state", state), zap.Error(err))
			continue
		}
		result = filterToProductColumns(result, nwiProduct)

		for _, shpRow := range result.Rows {
			row, ok := newWetlandRow(shpRow)
			if !ok {
				continue
			}
			batch = append(batch, row)
			if len(batch) >= nwiBatchSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("NWI wetlands sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// findShpFile finds the first .shp file in a directory.
func findShpFile(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", eris.Wrap(err, "read directory")
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".shp") {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", eris.Errorf("no .shp file found in %s", dir)
}
