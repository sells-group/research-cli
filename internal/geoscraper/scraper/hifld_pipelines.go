package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// pipelineExclude lists attribute keys stored in dedicated columns.
var pipelineExclude = map[string]bool{
	"OBJECTID":     true,
	"Operator":     true,
	"Shape_Length": true,
}

// HIFLDPipelines scrapes natural gas pipeline data from the HIFLD ArcGIS service.
// Pipelines are polyline geometries; the centroid is stored as lat/lon and the
// bounding box is included in the properties JSONB.
type HIFLDPipelines struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDPipelines) Name() string { return "hifld_pipelines" }

// Table implements GeoScraper.
func (h *HIFLDPipelines) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDPipelines) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDPipelines) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *HIFLDPipelines) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDPipelines) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting pipelines sync")

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
			return eris.Wrap(err, "pipelines: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Natural_Gas_Pipelines"),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			// Build properties with bbox for line geometry.
			props := make(map[string]any)
			for k, v := range feat.Attributes {
				if pipelineExclude[k] || v == nil {
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

			name := hifldString(feat.Attributes, "Operator")
			if name == "" {
				name = fmt.Sprintf("Pipeline %s", sourceID)
			}

			row := []any{
				name,
				"pipeline",
				"natural_gas",
				0.0, // no capacity field for pipelines
				lat,
				lon,
				hifldSource,
				sourceID,
				propsJSON,
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
		return nil, eris.Wrap(err, "pipelines: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("pipelines sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
