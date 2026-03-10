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
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// coalMineExclude lists attribute keys stored in dedicated columns.
var coalMineExclude = map[string]bool{
	"OBJECTID":  true,
	"MINE_NAME": true,
	"MINE_TYPE": true,
}

// USGSCoalMines scrapes coal mine locations from the USGS coal mines ArcGIS service.
type USGSCoalMines struct {
	baseURL string // override for testing; empty uses default coal mines endpoint
}

// Name implements GeoScraper.
func (h *USGSCoalMines) Name() string { return "usgs_coal_mines" }

// Table implements GeoScraper.
func (h *USGSCoalMines) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *USGSCoalMines) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *USGSCoalMines) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *USGSCoalMines) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (h *USGSCoalMines) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting coal mines sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        h.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "coal_mines: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: usgsURL(h.baseURL, coalMinesBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			row := []any{
				hifldString(feat.Attributes, "MINE_NAME"),
				"coal_mine",
				hifldString(feat.Attributes, "MINE_TYPE"),
				0.0,
				lat,
				lon,
				usgsSource,
				sourceID,
				hifldProperties(feat.Attributes, coalMineExclude),
			}
			batch = append(batch, row)

			if len(batch) >= usgsBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, eris.Wrap(err, "coal_mines: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("coal mines sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
