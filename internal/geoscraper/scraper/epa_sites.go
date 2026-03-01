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

// EPASites scrapes facility locations from the EPA Facility Registry Service ArcGIS MapServer.
type EPASites struct {
	baseURL string // override for testing; empty uses default EPA endpoint
}

// Name implements GeoScraper.
func (e *EPASites) Name() string { return "epa_sites" }

// Table implements GeoScraper.
func (e *EPASites) Table() string { return "geo.epa_sites" }

// Category implements GeoScraper.
func (e *EPASites) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (e *EPASites) Cadence() geoscraper.Cadence { return geoscraper.Monthly }

// ShouldRun implements GeoScraper.
func (e *EPASites) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.MonthlySchedule(now, lastSync)
}

// Sync implements GeoScraper.
func (e *EPASites) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", e.Name()))
	log.Info("starting EPA sites sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        e.Table(),
			Columns:      epaCols,
			ConflictKeys: epaConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "epa_sites: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, state := range stateAbbrevs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Debug("querying state", zap.String("state", state))

		err := arcgis.QueryAll(ctx, ft, arcgis.QueryConfig{
			BaseURL: epaURL(e.baseURL),
			Where:   buildEPAWhere(state),
		}, func(features []arcgis.Feature) error {
			for _, feat := range features {
				row, ok := newEPARow(feat)
				if !ok {
					continue
				}

				batch = append(batch, row)

				if len(batch) >= epaBatchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, eris.Wrapf(err, "epa_sites: query state %s", state)
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("EPA sites sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// newEPARow builds a row for the epa_sites table from an ArcGIS feature.
// Returns nil, false if the feature has no geometry or no REGISTRY_ID.
func newEPARow(feat arcgis.Feature) ([]any, bool) {
	if feat.Geometry == nil {
		return nil, false
	}

	registryID := hifldString(feat.Attributes, "REGISTRY_ID")
	if registryID == "" {
		return nil, false
	}

	lat, lon := feat.Geometry.Centroid()
	sourceID := fmt.Sprintf("%v", feat.Attributes["REGISTRY_ID"])

	return []any{
		hifldString(feat.Attributes, "PRIMARY_NAME"),
		hifldString(feat.Attributes, "PGM_SYS_ACRNM"),
		registryID,
		hifldString(feat.Attributes, "ACTIVE_STATUS"),
		lat,
		lon,
		epaSource,
		sourceID,
		hifldProperties(feat.Attributes, epaExclude),
	}, true
}
