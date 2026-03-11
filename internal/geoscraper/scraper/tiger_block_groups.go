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

// blockGroupProduct defines the shapefile columns for Census block groups.
var blockGroupProduct = tiger.Product{
	Name:     "BG",
	Table:    "bg",
	Columns:  []string{"statefp", "countyfp", "tractce", "blkgrpce", "geoid", "namelsad", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var blockGroupCols = []string{
	"geoid", "state_fips", "county_fips", "tract_ce", "blkgrp_ce",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

// TIGERBlockGroups scrapes Census block group boundaries from TIGER/Line shapefiles.
type TIGERBlockGroups struct {
	downloadBaseURL string   // override for testing; empty uses census.gov
	year            int      // override for testing; 0 uses tigerYear
	stateFIPS       []string // override for testing; empty uses AllStateFIPS()
}

// Name implements GeoScraper.
func (t *TIGERBlockGroups) Name() string { return "tiger_block_groups" }

// Table implements GeoScraper.
func (t *TIGERBlockGroups) Table() string { return "geo.block_groups" }

// Category implements GeoScraper.
func (t *TIGERBlockGroups) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (t *TIGERBlockGroups) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (t *TIGERBlockGroups) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (t *TIGERBlockGroups) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", t.Name()))
	log.Info("starting TIGER block groups sync")

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
			log.Warn("block group download failed, skipping state",
				zap.String("fips", fips), zap.Error(err))
			continue
		}
		totalRows += n
	}

	log.Info("TIGER block groups sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (t *TIGERBlockGroups) buildURL(year int, fips string) string {
	filename := fmt.Sprintf("tl_%d_%s_bg.zip", year, fips)
	suffix := fmt.Sprintf("BG/%s", filename)
	return tigerURL(t.downloadBaseURL, year, suffix)
}

func (t *TIGERBlockGroups) downloadAndLoad(ctx context.Context, pool db.Pool, url, tempDir string) (int64, error) {
	shpPath, err := tiger.Download(ctx, url, tempDir)
	if err != nil {
		return 0, eris.Wrap(err, "download block groups")
	}

	result, err := tiger.ParseShapefile(shpPath, blockGroupProduct)
	if err != nil {
		return 0, eris.Wrap(err, "parse block groups shapefile")
	}
	result = filterToProductColumns(result, blockGroupProduct)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "geo.block_groups",
			Columns:      blockGroupCols,
			ConflictKeys: []string{"geoid"},
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "block_groups: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, raw := range result.Rows {
		row := newBlockGroupRow(raw)
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

func newBlockGroupRow(raw []any) []any {
	// raw: statefp, countyfp, tractce, blkgrpce, geoid, namelsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 4)
	lat, lon := parseLatLon(raw, 10, 11)
	props := boundaryProperties(raw,
		"namelsad", strVal(raw, 5),
		"mtfcc", strVal(raw, 6),
		"funcstat", strVal(raw, 7),
		"aland", strVal(raw, 8),
		"awater", strVal(raw, 9),
	)
	return []any{
		geoid,
		strVal(raw, 0),      // state_fips
		strVal(raw, 1),      // county_fips
		strVal(raw, 2),      // tract_ce
		strVal(raw, 3),      // blkgrp_ce
		wkbToWGS84(raw[12]), // geom (WKB, SRID 4269→4326)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}
