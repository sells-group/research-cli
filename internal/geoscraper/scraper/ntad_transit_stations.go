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

const ntadTransitStationsBaseURL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NTAD_National_Transit_Map_Stops/FeatureServer/0/query"

// transitStopTypeMap maps NTM stop_type codes to infrastructure type strings.
var transitStopTypeMap = map[string]string{
	"0": "light_rail_station",
	"1": "subway_station",
	"2": "commuter_rail_station",
}

// ntadTransitExclude lists attribute keys stored in dedicated columns.
var ntadTransitExclude = map[string]bool{
	"OBJECTID":  true,
	"stop_name": true,
	"stop_lat":  true,
	"stop_lon":  true,
	"stop_type": true,
}

// NTADTransitStations scrapes transit rail station data from the BTS NTAD ArcGIS service.
// Only tram/light rail (0), subway/metro (1), and commuter rail (2) stops are included.
type NTADTransitStations struct {
	baseURL string // override for testing; empty uses default BTS endpoint
}

// Name implements GeoScraper.
func (s *NTADTransitStations) Name() string { return "ntad_transit_stations" }

// Table implements GeoScraper.
func (s *NTADTransitStations) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *NTADTransitStations) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *NTADTransitStations) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *NTADTransitStations) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *NTADTransitStations) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting ntad_transit_stations sync")

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
			return eris.Wrap(err, "ntad_transit_stations: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(s.baseURL, ntadTransitStationsBaseURL),
		Where:   "stop_type IN ('0','1','2')",
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			stopType := fmt.Sprintf("%v", feat.Attributes["stop_type"])
			infraType, ok := transitStopTypeMap[stopType]
			if !ok {
				continue
			}

			row := []any{
				hifldString(feat.Attributes, "stop_name"),
				infraType,
				"",
				0.0,
				lat,
				lon,
				"bts",
				sourceID,
				hifldProperties(feat.Attributes, ntadTransitExclude),
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
		return nil, eris.Wrap(err, "ntad_transit_stations: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("ntad_transit_stations sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
