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

const ntadPortsBaseURL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NTAD_Commercial_Strategic_Seaports/FeatureServer/0/query"

// ntadPortExclude lists attribute keys stored in dedicated columns.
var ntadPortExclude = map[string]bool{
	"OBJECTID":  true,
	"port_name": true,
	"total":     true,
	"latitude":  true,
	"longitude": true,
}

// NTADPorts scrapes port data from the BTS NTAD ArcGIS service.
type NTADPorts struct {
	baseURL string // override for testing; empty uses default BTS endpoint
}

// Name implements GeoScraper.
func (s *NTADPorts) Name() string { return "ntad_ports" }

// Table implements GeoScraper.
func (s *NTADPorts) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *NTADPorts) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *NTADPorts) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *NTADPorts) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *NTADPorts) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting ntad_ports sync")

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
			return eris.Wrap(err, "ntad_ports: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(s.baseURL, ntadPortsBaseURL),
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
				hifldString(feat.Attributes, "port_name"),
				"port",
				"",
				hifldFloat64(feat.Attributes, "total"),
				lat,
				lon,
				"bts",
				sourceID,
				hifldProperties(feat.Attributes, ntadPortExclude),
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
		return nil, eris.Wrap(err, "ntad_ports: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("ntad_ports sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
