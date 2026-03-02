package scraper

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/overpass"
)

// OSMPOI scrapes points of interest from the OpenStreetMap Overpass API.
type OSMPOI struct {
	endpointURL string // override for testing; empty uses default Overpass endpoint
}

// Name implements GeoScraper.
func (s *OSMPOI) Name() string { return "osm_poi" }

// Table implements GeoScraper.
func (s *OSMPOI) Table() string { return "geo.poi" }

// Category implements GeoScraper.
func (s *OSMPOI) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *OSMPOI) Cadence() geoscraper.Cadence { return geoscraper.Monthly }

// ShouldRun implements GeoScraper.
func (s *OSMPOI) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.MonthlySchedule(now, lastSync)
}

// Sync implements GeoScraper.
func (s *OSMPOI) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting OSM POI sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      poiCols,
			ConflictKeys: poiConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "osm_poi: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	tiles := usTiles()
	for i, tile := range tiles {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		query := overpass.BuildPOIQuery(tile.south, tile.west, tile.north, tile.east)
		elems, err := overpass.Query(ctx, s.endpointURL, query)
		if err != nil {
			log.Warn("tile query failed, skipping",
				zap.Int("tile", i),
				zap.Float64("south", tile.south),
				zap.Float64("west", tile.west),
				zap.Error(err))
			continue
		}

		for _, elem := range elems {
			row, ok := newPOIRow(elem)
			if !ok {
				continue
			}

			batch = append(batch, row)

			if len(batch) >= osmBatchSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("OSM POI sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
