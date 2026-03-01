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

// powerPlantExclude lists attribute keys stored in dedicated columns.
var powerPlantExclude = map[string]bool{
	"OBJECTID":    true,
	"NAME":        true,
	"TYPE":        true,
	"CAPACITY_MW": true,
}

// HIFLDPowerPlants scrapes power plant locations from the HIFLD ArcGIS service.
type HIFLDPowerPlants struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDPowerPlants) Name() string { return "hifld_power_plants" }

// Table implements GeoScraper.
func (h *HIFLDPowerPlants) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDPowerPlants) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDPowerPlants) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *HIFLDPowerPlants) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDPowerPlants) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting power plants sync")

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
			return eris.Wrap(err, "power_plants: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Power_Plants"),
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
				hifldString(feat.Attributes, "NAME"),
				"power_plant",
				hifldString(feat.Attributes, "TYPE"),
				hifldFloat64(feat.Attributes, "CAPACITY_MW"),
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, powerPlantExclude),
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
		return nil, eris.Wrap(err, "power_plants: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("power plants sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
