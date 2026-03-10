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

// epaCWABaseURL is the EPA Clean Water Act wastewater MapServer endpoint.
const epaCWABaseURL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_Wastewater/MapServer/0/query"

// wastewaterExclude lists attribute keys stored in dedicated columns.
var wastewaterExclude = map[string]bool{
	"OBJECTID":   true,
	"OBJECTID_1": true,
	"SOURCE_ID":  true,
	"CWP_NAME":   true,
}

// EPAWastewater scrapes wastewater treatment plant locations from the EPA CWA ArcGIS service.
type EPAWastewater struct {
	baseURL string // override for testing; empty uses default EPA CWA endpoint
}

// Name implements GeoScraper.
func (e *EPAWastewater) Name() string { return "epa_wastewater" }

// Table implements GeoScraper.
func (e *EPAWastewater) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (e *EPAWastewater) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (e *EPAWastewater) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (e *EPAWastewater) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (e *EPAWastewater) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", e.Name()))
	log.Info("starting wastewater plants sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        e.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "epa_wastewater: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	baseURL := epaCWABaseURL
	if e.baseURL != "" {
		baseURL = e.baseURL
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL: baseURL,
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			if feat.Geometry == nil {
				log.Warn("skipping feature with null geometry",
					zap.Any("objectid", feat.Attributes["OBJECTID"]))
				continue
			}

			sourceIDVal := hifldString(feat.Attributes, "SOURCE_ID")
			if sourceIDVal == "" {
				continue
			}

			lat, lon := feat.Geometry.Centroid()
			sourceID := sourceIDVal

			row := []any{
				hifldString(feat.Attributes, "CWP_NAME"),
				"wastewater_plant",
				"",
				0.0,
				lat,
				lon,
				"epa",
				sourceID,
				hifldProperties(feat.Attributes, wastewaterExclude),
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
		return nil, eris.Wrap(err, "epa_wastewater: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("wastewater plants sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
