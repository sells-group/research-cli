package scraper

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

// cousubProduct defines the shapefile columns for Census county subdivisions.
var cousubProduct = tiger.Product{
	Name:     "COUSUB",
	Table:    "cousub",
	Columns:  []string{"statefp", "countyfp", "cousubfp", "cousubns", "geoid", "name", "namelsad", "lsad", "classfp", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var cousubCols = []string{
	"geoid", "state_fips", "county_fips", "cousub_fips", "name", "lsad", "class_fips",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

// TIGERCousub scrapes county subdivision boundaries from Census TIGER/Line shapefiles.
type TIGERCousub struct {
	downloadBaseURL string   // override for testing; empty uses census.gov
	year            int      // override for testing; 0 uses tigerYear
	stateFIPS       []string // override for testing; empty uses AllStateFIPS()
}

// Name implements GeoScraper.
func (t *TIGERCousub) Name() string { return "tiger_cousub" }

// Table implements GeoScraper.
func (t *TIGERCousub) Table() string { return "geo.county_subdivisions" }

// Category implements GeoScraper.
func (t *TIGERCousub) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (t *TIGERCousub) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (t *TIGERCousub) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (t *TIGERCousub) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", t.Name()))
	log.Info("starting TIGER county subdivisions sync")

	year := tigerYear
	if t.year > 0 {
		year = t.year
	}

	var totalRows int64

	states := tiger.AllStateFIPS()
	if len(t.stateFIPS) > 0 {
		states = t.stateFIPS
	}

	for _, fips := range states {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := t.buildURL(year, fips)
		n, err := t.downloadAndLoad(ctx, pool, url, tempDir)
		if err != nil {
			if strings.Contains(err.Error(), "upsert") {
				return nil, err
			}
			log.Warn("cousub download failed, skipping state",
				zap.String("fips", fips), zap.Error(err))
			continue
		}
		totalRows += n
	}

	log.Info("TIGER county subdivisions sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (t *TIGERCousub) buildURL(year int, fips string) string {
	filename := fmt.Sprintf("tl_%d_%s_cousub.zip", year, fips)
	suffix := fmt.Sprintf("COUSUB/%s", filename)
	return tigerURL(t.downloadBaseURL, year, suffix)
}

func (t *TIGERCousub) downloadAndLoad(ctx context.Context, pool db.Pool, url, tempDir string) (int64, error) {
	shpPath, err := tiger.Download(ctx, url, tempDir)
	if err != nil {
		return 0, eris.Wrap(err, "download cousub")
	}

	result, err := tiger.ParseShapefile(shpPath, cousubProduct)
	if err != nil {
		return 0, eris.Wrap(err, "parse cousub shapefile")
	}
	result = filterToProductColumns(result, cousubProduct)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "geo.county_subdivisions",
			Columns:      cousubCols,
			ConflictKeys: []string{"geoid"},
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "cousub: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, raw := range result.Rows {
		row := newCousubRow(raw)
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

func newCousubRow(raw []any) []any {
	// raw: statefp, countyfp, cousubfp, cousubns, geoid, name, namelsad, lsad, classfp, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 4)
	lat, lon := parseLatLon(raw, 13, 14)
	props := boundaryProperties(raw,
		"namelsad", strVal(raw, 6),
		"cousubns", strVal(raw, 3),
		"mtfcc", strVal(raw, 9),
		"funcstat", strVal(raw, 10),
		"aland", strVal(raw, 11),
		"awater", strVal(raw, 12),
	)
	return []any{
		geoid,
		strVal(raw, 0), // state_fips
		strVal(raw, 1), // county_fips
		strVal(raw, 2), // cousub_fips
		strVal(raw, 5), // name
		strVal(raw, 7), // lsad
		strVal(raw, 8), // class_fips
		raw[15],        // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}
