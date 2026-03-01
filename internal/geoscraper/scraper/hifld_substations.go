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

// substationExclude lists attribute keys stored in dedicated columns.
var substationExclude = map[string]bool{
	"OBJECTID": true,
	"NAME":     true,
	"MAX_VOLT": true,
}

// HIFLDSubstations scrapes electric substation locations from the HIFLD ArcGIS service.
type HIFLDSubstations struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDSubstations) Name() string { return "hifld_substations" }

// Table implements GeoScraper.
func (h *HIFLDSubstations) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDSubstations) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDSubstations) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (h *HIFLDSubstations) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDSubstations) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting substations sync")

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
			return eris.Wrap(err, "substations: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Electric_Substations"),
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
				"substation",
				"", // no fuel_type for substations
				hifldFloat64(feat.Attributes, "MAX_VOLT"),
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, substationExclude),
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
		return nil, eris.Wrap(err, "substations: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("substations sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
