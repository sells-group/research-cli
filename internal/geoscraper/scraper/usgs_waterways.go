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

// waterwayExclude lists attribute keys stored in dedicated columns.
var waterwayExclude = map[string]bool{
	"OBJECTID":   true,
	"GNIS_Name":  true,
	"FTYPE_DESC": true,
	"LENGTHKM":   true,
}

// USGSWaterways scrapes waterway features from the USGS National Hydrography Dataset ArcGIS service.
type USGSWaterways struct {
	baseURL string // override for testing; empty uses default NHD endpoint
}

// Name implements GeoScraper.
func (h *USGSWaterways) Name() string { return "usgs_waterways" }

// Table implements GeoScraper.
func (h *USGSWaterways) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *USGSWaterways) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *USGSWaterways) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *USGSWaterways) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (h *USGSWaterways) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting waterways sync")

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
			return eris.Wrap(err, "waterways: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: usgsURL(h.baseURL, nhdBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			name := hifldString(feat.Attributes, "GNIS_Name")
			if name == "" {
				name = fmt.Sprintf("Waterway %s", sourceID)
			}

			// Build properties with bbox for polyline geometry.
			props := make(map[string]any)
			for k, v := range feat.Attributes {
				if waterwayExclude[k] || v == nil {
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
				name,
				"waterway",
				hifldString(feat.Attributes, "FTYPE_DESC"),
				hifldFloat64(feat.Attributes, "LENGTHKM"),
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
		return nil, eris.Wrap(err, "waterways: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("waterways sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
