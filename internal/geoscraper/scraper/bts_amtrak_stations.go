package scraper

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

const btsAmtrakBaseURL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NTAD_Amtrak_Stations/FeatureServer/0/query"

// btsAmtrakExclude lists attribute keys stored in dedicated columns.
var btsAmtrakExclude = map[string]bool{
	"OBJECTID":    true,
	"Code":        true,
	"StationName": true,
	"StnType":     true,
	"lat":         true,
	"lon":         true,
}

// BTSAmtrakStations scrapes Amtrak station data from the BTS NTAD ArcGIS service.
type BTSAmtrakStations struct {
	baseURL string // override for testing; empty uses default BTS endpoint
}

// Name implements GeoScraper.
func (s *BTSAmtrakStations) Name() string { return "bts_amtrak_stations" }

// Table implements GeoScraper.
func (s *BTSAmtrakStations) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *BTSAmtrakStations) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *BTSAmtrakStations) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *BTSAmtrakStations) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *BTSAmtrakStations) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting bts_amtrak_stations sync")

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
			return eris.Wrap(err, "bts_amtrak_stations: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(s.baseURL, btsAmtrakBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			sourceID := hifldString(feat.Attributes, "Code")
			if sourceID == "" {
				continue
			}

			lat, lon := feat.Geometry.Centroid()

			row := []any{
				hifldString(feat.Attributes, "StationName"),
				"amtrak_station",
				hifldString(feat.Attributes, "StnType"),
				0.0,
				lat,
				lon,
				"bts",
				sourceID,
				hifldProperties(feat.Attributes, btsAmtrakExclude),
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
		return nil, eris.Wrap(err, "bts_amtrak_stations: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("bts_amtrak_stations sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
