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

// FEMAFloodZones scrapes flood hazard areas from FEMA's NFHL ArcGIS FeatureServer.
type FEMAFloodZones struct {
	baseURL string // override for testing; empty uses default FEMA endpoint
}

// Name implements GeoScraper.
func (f *FEMAFloodZones) Name() string { return "fema_flood" }

// Table implements GeoScraper.
func (f *FEMAFloodZones) Table() string { return "geo.flood_zones" }

// Category implements GeoScraper.
func (f *FEMAFloodZones) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (f *FEMAFloodZones) Cadence() geoscraper.Cadence { return geoscraper.Monthly }

// ShouldRun implements GeoScraper.
func (f *FEMAFloodZones) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.MonthlySchedule(now, lastSync)
}

// Sync implements GeoScraper.
func (f *FEMAFloodZones) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", f.Name()))
	log.Info("starting FEMA flood zones sync")

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := floodUpsert(ctx, pool, f.Table(), batch)
		if err != nil {
			return eris.Wrap(err, "fema_flood: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, fips := range stateFIPS {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Debug("querying state", zap.String("fips", fips))

		err := arcgis.QueryAll(ctx, ft, arcgis.QueryConfig{
			BaseURL: femaURL(f.baseURL),
			Where:   buildFEMAWhere(fips),
			OutSR:   4326,
		}, func(features []arcgis.Feature) error {
			for _, feat := range features {
				row, ok := newFloodRow(feat)
				if !ok {
					log.Warn("skipping feature with null/empty geometry",
						zap.Any("objectid", feat.Attributes["OBJECTID"]))
					continue
				}

				batch = append(batch, row)

				if len(batch) >= femaBatchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, eris.Wrapf(err, "fema_flood: query state %s", fips)
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("FEMA flood zones sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
