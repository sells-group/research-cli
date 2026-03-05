package scraper

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

// TIGERRoads scrapes primary road geometries from Census TIGER/Line shapefiles.
type TIGERRoads struct {
	downloadURL string // override for testing; empty uses census.gov
	year        int    // override for testing; 0 uses tigerYear
}

// Name implements GeoScraper.
func (r *TIGERRoads) Name() string { return "tiger_roads" }

// Table implements GeoScraper.
func (r *TIGERRoads) Table() string { return "geo.roads" }

// Category implements GeoScraper.
func (r *TIGERRoads) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (r *TIGERRoads) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (r *TIGERRoads) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (r *TIGERRoads) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", r.Name()))
	log.Info("starting TIGER roads sync")

	year := r.effectiveYear()
	url := r.buildURL(year)

	shpPath, err := tiger.Download(ctx, url, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "tiger_roads: download")
	}

	result, err := tiger.ParseShapefile(shpPath, tigerRoadProduct)
	if err != nil {
		return nil, eris.Wrap(err, "tiger_roads: parse shapefile")
	}
	result = filterToProductColumns(result, tigerRoadProduct)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        r.Table(),
			Columns:      roadCols,
			ConflictKeys: roadConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "tiger_roads: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, raw := range result.Rows {
		row := newRoadRow(raw)
		batch = append(batch, row)
		if len(batch) >= tigerBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("TIGER roads sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (r *TIGERRoads) effectiveYear() int {
	if r.year > 0 {
		return r.year
	}
	return tigerYear
}

func (r *TIGERRoads) buildURL(year int) string {
	if r.downloadURL != "" {
		return r.downloadURL
	}
	return fmt.Sprintf(
		"https://www2.census.gov/geo/tiger/TIGER%d/PRIMARYROADS/tl_%d_us_primaryroads.zip",
		year, year,
	)
}
