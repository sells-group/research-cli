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

// transmissionExclude lists attribute keys stored in dedicated columns.
var transmissionExclude = map[string]bool{
	"OBJECTID": true,
	"OWNER":    true,
	"VOLTAGE":  true,
}

// HIFLDTransmissionLines scrapes electric transmission line data from the HIFLD ArcGIS service.
// Lines are polyline geometries; the centroid is stored as lat/lon and the bounding box
// is included in the properties JSONB.
type HIFLDTransmissionLines struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDTransmissionLines) Name() string { return "hifld_transmission_lines" }

// Table implements GeoScraper.
func (h *HIFLDTransmissionLines) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDTransmissionLines) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDTransmissionLines) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *HIFLDTransmissionLines) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDTransmissionLines) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting transmission lines sync")

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
			return eris.Wrap(err, "transmission_lines: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Electric_Power_Transmission_Lines"),
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
				if transmissionExclude[k] || v == nil {
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

			name := hifldString(feat.Attributes, "OWNER")
			if name == "" {
				name = fmt.Sprintf("Transmission Line %s", sourceID)
			}

			row := []any{
				name,
				"transmission_line",
				"", // no fuel_type
				hifldFloat64(feat.Attributes, "VOLTAGE"),
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
		return nil, eris.Wrap(err, "transmission_lines: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("transmission lines sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
