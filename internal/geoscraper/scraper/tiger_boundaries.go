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
	"github.com/sells-group/research-cli/internal/tiger"
)

// TIGERBoundaries scrapes county, place, ZCTA, CBSA, census tract, and
// congressional district boundaries from Census TIGER/Line shapefiles.
type TIGERBoundaries struct {
	downloadBaseURL string // override for testing; empty uses census.gov
	year            int    // override for testing; 0 uses tigerYear
}

// Name implements GeoScraper.
func (t *TIGERBoundaries) Name() string { return "tiger_boundaries" }

// Table implements GeoScraper.
func (t *TIGERBoundaries) Table() string { return "geo.counties" }

// Category implements GeoScraper.
func (t *TIGERBoundaries) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (t *TIGERBoundaries) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (t *TIGERBoundaries) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (t *TIGERBoundaries) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", t.Name()))
	log.Info("starting TIGER boundaries sync")

	year := t.effectiveYear()
	var totalRows int64

	for _, def := range boundaryDefs() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		n, err := t.syncBoundary(ctx, pool, def, year, tempDir)
		if err != nil {
			return nil, eris.Wrapf(err, "tiger_boundaries: sync %s", def.name)
		}
		totalRows += n
		log.Info("boundary sync complete",
			zap.String("boundary", def.name),
			zap.Int64("rows", n),
		)
	}

	log.Info("TIGER boundaries sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (t *TIGERBoundaries) effectiveYear() int {
	if t.year > 0 {
		return t.year
	}
	return tigerYear
}

func (t *TIGERBoundaries) syncBoundary(ctx context.Context, pool db.Pool, def boundaryDef, year int, tempDir string) (int64, error) {
	if def.national {
		return t.syncNational(ctx, pool, def, year, tempDir)
	}
	return t.syncPerState(ctx, pool, def, year, tempDir)
}

func (t *TIGERBoundaries) syncNational(ctx context.Context, pool db.Pool, def boundaryDef, year int, tempDir string) (int64, error) {
	url := t.buildURL(def, year, "")
	return t.downloadAndLoad(ctx, pool, def, url, tempDir)
}

func (t *TIGERBoundaries) syncPerState(ctx context.Context, pool db.Pool, def boundaryDef, year int, tempDir string) (int64, error) {
	log := zap.L().With(zap.String("scraper", t.Name()), zap.String("boundary", def.name))
	var totalRows int64

	for _, fips := range tiger.AllStateFIPS() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		url := t.buildURL(def, year, fips)
		n, err := t.downloadAndLoad(ctx, pool, def, url, tempDir)
		if err != nil {
			log.Warn("per-state download failed, skipping",
				zap.String("fips", fips),
				zap.Error(err),
			)
			continue
		}
		totalRows += n
	}

	return totalRows, nil
}

func (t *TIGERBoundaries) buildURL(def boundaryDef, year int, fips string) string {
	var filename string
	if def.national {
		filename = fmt.Sprintf("tl_%d_us_%s.zip", year, def.product.Table)
	} else {
		filename = fmt.Sprintf("tl_%d_%s_%s.zip", year, fips, def.product.Table)
	}
	suffix := fmt.Sprintf("%s/%s", def.product.Name, filename)
	return tigerURL(t.downloadBaseURL, year, suffix)
}

func (t *TIGERBoundaries) downloadAndLoad(ctx context.Context, pool db.Pool, def boundaryDef, url, tempDir string) (int64, error) {
	shpPath, err := tiger.Download(ctx, url, tempDir)
	if err != nil {
		return 0, eris.Wrapf(err, "download %s", def.name)
	}

	result, err := tiger.ParseShapefile(shpPath, def.product)
	if err != nil {
		return 0, eris.Wrapf(err, "parse %s shapefile", def.name)
	}

	result = filterToProductColumns(result, def.product)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        def.table,
			Columns:      def.columns,
			ConflictKeys: []string{def.conflictKey},
		}, batch)
		if uErr != nil {
			return eris.Wrapf(uErr, "%s: upsert batch", def.name)
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, raw := range result.Rows {
		row := def.buildRow(raw)
		batch = append(batch, row)
		if len(batch) >= tigerBatchSize {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}

	return totalRows, nil
}
