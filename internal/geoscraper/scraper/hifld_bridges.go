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

// bridgeExclude lists attribute keys stored in dedicated columns.
var bridgeExclude = map[string]bool{
	"OBJECTID":             true,
	"STRUCTURE_NUMBER_008": true,
	"FACILITY_CARRIED_007": true,
	"YEAR_BUILT_027":       true,
	"DECK_AREA":            true,
}

// HIFLDBridges scrapes bridge locations from the HIFLD ArcGIS service.
type HIFLDBridges struct {
	baseURL string // override for testing; empty uses default HIFLD endpoint
}

// Name implements GeoScraper.
func (h *HIFLDBridges) Name() string { return "hifld_bridges" }

// Table implements GeoScraper.
func (h *HIFLDBridges) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (h *HIFLDBridges) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (h *HIFLDBridges) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (h *HIFLDBridges) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return hifldAnnualShouldRun(now, lastSync)
}

// Sync implements GeoScraper.
func (h *HIFLDBridges) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", h.Name()))
	log.Info("starting bridges sync")

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
			return eris.Wrap(err, "bridges: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(h.baseURL, bridgesBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["STRUCTURE_NUMBER_008"])

			row := []any{
				hifldString(feat.Attributes, "FACILITY_CARRIED_007"),
				"bridge",
				hifldString(feat.Attributes, "YEAR_BUILT_027"),
				hifldFloat64(feat.Attributes, "DECK_AREA"),
				lat,
				lon,
				hifldSource,
				sourceID,
				hifldProperties(feat.Attributes, bridgeExclude),
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
		return nil, eris.Wrap(err, "bridges: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("bridges sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
