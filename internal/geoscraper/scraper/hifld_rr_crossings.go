package scraper

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// rrCrossingExclude lists attribute keys stored in dedicated columns.
var rrCrossingExclude = map[string]bool{
	"OBJECTID": true,
	"CROSSING": true,
	"TYPEXING": true,
}

// HIFLDRRCrossings scrapes highway-rail grade crossing locations from the HIFLD ArcGIS service.
type HIFLDRRCrossings struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDRRCrossings) Name() string { return "hifld_rr_crossings" }

// Table implements GeoScraper.
func (h *HIFLDRRCrossings) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDRRCrossings) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDRRCrossings) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *HIFLDRRCrossings) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDRRCrossings) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting rr_crossings sync")

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
			return eris.Wrap(err, "rr_crossings: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Highway_Rail_Grade_Crossings"),
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
				hifldString(feat.Attributes, "CROSSING"),
				"rr_crossing",
				hifldString(feat.Attributes, "TYPEXING"),
				0.0,
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, rrCrossingExclude),
			}
			batch = append(batch, row)

			if len(batch) >= hifldBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, eris.Wrap(err, "rr_crossings: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("rr_crossings sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
