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

const fhwaHPMSBaseURL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/HPMS_Data/FeatureServer/0/query"

// fhwaHPMSExclude lists attribute keys stored in dedicated columns.
var fhwaHPMSExclude = map[string]bool{
	"OBJECTID":      true,
	"route_id":      true,
	"state_code":    true,
	"begin_poin":    true,
	"aadt":          true,
	"f_system":      true,
	"Shape__Length": true,
}

// FHWAHPMs scrapes highway performance data from the FHWA HPMS ArcGIS service.
type FHWAHPMs struct {
	baseURL string // override for testing; empty uses default FHWA endpoint
}

// Name implements GeoScraper.
func (s *FHWAHPMs) Name() string { return "fhwa_hpms" }

// Table implements GeoScraper.
func (s *FHWAHPMs) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *FHWAHPMs) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *FHWAHPMs) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *FHWAHPMs) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *FHWAHPMs) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting fhwa_hpms sync")

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
			return eris.Wrap(err, "fhwa_hpms: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: hifldURL(s.baseURL, fhwaHPMSBaseURL),
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			stateCode := fmt.Sprintf("%v", feat.Attributes["state_code"])
			routeID := fmt.Sprintf("%v", feat.Attributes["route_id"])
			beginPt := fmt.Sprintf("%v", feat.Attributes["begin_poin"])
			if beginPt == "<nil>" {
				beginPt = ""
			}
			sourceID := fmt.Sprintf("%s_%s_%s", stateCode, routeID, beginPt)
			if stateCode == "" || stateCode == "<nil>" || routeID == "" || routeID == "<nil>" {
				continue
			}

			lat, lon := feat.Geometry.Centroid()

			fSystem := fmt.Sprintf("%v", feat.Attributes["f_system"])
			if fSystem == "<nil>" {
				fSystem = ""
			}

			row := []any{
				routeID,
				"highway_segment",
				fSystem,
				hifldFloat64(feat.Attributes, "aadt"),
				lat,
				lon,
				"fhwa",
				sourceID,
				hifldProperties(feat.Attributes, fhwaHPMSExclude),
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
		return nil, eris.Wrap(err, "fhwa_hpms: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("fhwa_hpms sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
