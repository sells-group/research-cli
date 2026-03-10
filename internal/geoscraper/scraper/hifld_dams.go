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

// damExclude lists attribute keys stored in dedicated columns.
var damExclude = map[string]bool{
	"OBJECTID":   true,
	"NAME":       true,
	"DAM_TYPE":   true,
	"NID_HEIGHT": true,
}

// HIFLDDams scrapes dam locations from the HIFLD ArcGIS service.
type HIFLDDams struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDDams) Name() string { return "hifld_dams" }

// Table implements GeoScraper.
func (h *HIFLDDams) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDDams) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDDams) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *HIFLDDams) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldAnnualShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDDams) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting dams sync")

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
			return eris.Wrap(err, "dams: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "National_Inventory_of_Dams"),
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
				"dam",
				hifldString(feat.Attributes, "DAM_TYPE"),
				hifldFloat64(feat.Attributes, "NID_HEIGHT"),
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, damExclude),
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
		return nil, eris.Wrap(err, "dams: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("dams sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
