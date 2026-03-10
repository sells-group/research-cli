package scraper

import (
	"context"
	"encoding/json"
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

// protectedAreaExclude lists attribute keys stored in dedicated columns.
var protectedAreaExclude = map[string]bool{
	"OBJECTID":  true,
	"Unit_Nm":   true,
	"GAP_Sts":   true,
	"GIS_Acres": true,
}

// USGSProtectedAreas scrapes protected area boundaries from the PAD-US ArcGIS service.
type USGSProtectedAreas struct {
	baseURL string // override for testing; empty uses default PAD-US endpoint
}

// Name implements GeoScraper.
func (h *USGSProtectedAreas) Name() string { return "usgs_protected_areas" }

// Table implements GeoScraper.
func (h *USGSProtectedAreas) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *USGSProtectedAreas) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *USGSProtectedAreas) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *USGSProtectedAreas) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (h *USGSProtectedAreas) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting protected areas sync")

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
			return eris.Wrap(err, "protected_areas: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: usgsURL(h.baseURL, padusBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			// Build properties with bbox for polygon geometry.
			props := make(map[string]any)
			for k, v := range feat.Attributes {
				if protectedAreaExclude[k] || v == nil {
					continue
				}
				props[k] = v
			}
			if bbox := feat.Geometry.BBox(); bbox != nil {
				props["bbox"] = bbox
			}
			propsJSON, err := json.Marshal(props)
			if err != nil {
				propsJSON = []byte("{}")
			}

			row := []any{
				hifldString(feat.Attributes, "Unit_Nm"),
				"protected_area",
				hifldString(feat.Attributes, "GAP_Sts"),
				hifldFloat64(feat.Attributes, "GIS_Acres"),
				lat,
				lon,
				usgsSource,
				sourceID,
				propsJSON,
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
		return nil, eris.Wrap(err, "protected_areas: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("protected areas sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
