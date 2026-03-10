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

// cemeteryExclude lists attribute keys stored in dedicated columns.
var cemeteryExclude = map[string]bool{
	"OBJECTID": true,
	"NAME":     true,
}

// HIFLDCemeteries scrapes cemetery locations from the HIFLD ArcGIS service.
type HIFLDCemeteries struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDCemeteries) Name() string { return "hifld_cemeteries" }

// Table implements GeoScraper.
func (h *HIFLDCemeteries) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDCemeteries) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDCemeteries) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *HIFLDCemeteries) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldAnnualShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDCemeteries) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting cemeteries sync")

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
			return eris.Wrap(err, "cemeteries: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, "Cemeteries"),
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
				"cemetery",
				"",
				0.0,
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, cemeteryExclude),
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
		return nil, eris.Wrap(err, "cemeteries: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("cemeteries sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
