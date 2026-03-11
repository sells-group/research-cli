package scraper

import (
	"context"
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

const btsFreightRailBaseURL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NorthAmericanRailNetworkLines_exposure_d/FeatureServer/0/query"

// btsFreightRailExclude lists attribute keys stored in dedicated columns.
var btsFreightRailExclude = map[string]bool{
	"OBJECTID":      true,
	"fid":           true,
	"FRAARCID":      true,
	"STATEAB":       true,
	"PASSNGR":       true,
	"TRACKS":        true,
	"MILES":         true,
	"Shape__Length": true,
}

// BTSFreightRail scrapes freight rail network data from the BTS NTAD ArcGIS service.
type BTSFreightRail struct {
	baseURL string // override for testing; empty uses default BTS endpoint
}

// Name implements GeoScraper.
func (s *BTSFreightRail) Name() string { return "bts_freight_rail" }

// Table implements GeoScraper.
func (s *BTSFreightRail) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *BTSFreightRail) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *BTSFreightRail) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *BTSFreightRail) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *BTSFreightRail) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting bts_freight_rail sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "bts_freight_rail: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(s.baseURL, btsFreightRailBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			sourceID := fmt.Sprintf("%v", feat.Attributes["FRAARCID"])
			if sourceID == "" || sourceID == "<nil>" {
				continue
			}

			lat, lon := feat.Geometry.Centroid()

			stateAB := hifldString(feat.Attributes, "STATEAB")
			passngr := hifldString(feat.Attributes, "PASSNGR")

			row := []any{
				stateAB + " Rail Segment",
				"freight_rail",
				passngr,
				hifldFloat64(feat.Attributes, "MILES"),
				lat,
				lon,
				"bts",
				sourceID,
				hifldProperties(feat.Attributes, btsFreightRailExclude),
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
		return nil, eris.Wrap(err, "bts_freight_rail: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("bts_freight_rail sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
