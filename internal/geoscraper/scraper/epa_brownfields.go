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

// epaBrownfieldsBaseURL is the EPA Brownfields MapServer endpoint.
const epaBrownfieldsBaseURL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_Brownfields/MapServer/0/query"

// brownfieldExclude lists attribute keys stored in dedicated columns.
var brownfieldExclude = map[string]bool{
	"OBJECTID":      true,
	"NAME":          true,
	"PROPERTY_TYPE": true,
	"ACRES":         true,
}

// EPABrownfields scrapes brownfield site locations from the EPA Brownfields ArcGIS service.
type EPABrownfields struct {
	baseURL string // override for testing; empty uses default EPA Brownfields endpoint
}

// Name implements GeoScraper.
func (e *EPABrownfields) Name() string { return "epa_brownfields" }

// Table implements GeoScraper.
func (e *EPABrownfields) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (e *EPABrownfields) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (e *EPABrownfields) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (e *EPABrownfields) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (e *EPABrownfields) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", e.Name()))
	log.Info("starting brownfields sync")

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
			return eris.Wrap(err, "epa_brownfields: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	baseURL := epaBrownfieldsBaseURL
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

			lat, lon := feat.Geometry.Centroid()
			sourceID := fmt.Sprintf("%v", feat.Attributes["OBJECTID"])

			row := []any{
				hifldString(feat.Attributes, "NAME"),
				"brownfield",
				hifldString(feat.Attributes, "PROPERTY_TYPE"),
				hifldFloat64(feat.Attributes, "ACRES"),
				lat,
				lon,
				"epa",
				sourceID,
				hifldProperties(feat.Attributes, brownfieldExclude),
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
		return nil, eris.Wrap(err, "epa_brownfields: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("brownfields sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
