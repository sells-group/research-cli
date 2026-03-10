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

// oilGasWellExclude lists attribute keys stored in dedicated columns.
var oilGasWellExclude = map[string]bool{
	"OBJECTID":    true,
	"WELL_NAME":   true,
	"WELL_STATUS": true,
	"WELL_DEPTH":  true,
}

// USGSOilGasWells scrapes oil and natural gas well locations from the Esri Living Atlas ArcGIS service.
type USGSOilGasWells struct {
	baseURL string // override for testing; empty uses default oil/gas wells endpoint
}

// Name implements GeoScraper.
func (h *USGSOilGasWells) Name() string { return "usgs_oil_gas_wells" }

// Table implements GeoScraper.
func (h *USGSOilGasWells) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *USGSOilGasWells) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *USGSOilGasWells) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *USGSOilGasWells) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *USGSOilGasWells) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting oil/gas wells sync")

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
			return eris.Wrap(err, "oil_gas_wells: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: usgsURL(h.baseURL, oilGasWellsBaseURL),
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
				hifldString(feat.Attributes, "WELL_NAME"),
				"oil_gas_well",
				hifldString(feat.Attributes, "WELL_STATUS"),
				hifldFloat64(feat.Attributes, "WELL_DEPTH"),
				lat,
				lon,
				usgsSource,
				sourceID,
				hifldProperties(feat.Attributes, oilGasWellExclude),
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
		return nil, eris.Wrap(err, "oil_gas_wells: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("oil/gas wells sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
