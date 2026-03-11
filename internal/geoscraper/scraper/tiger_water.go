package scraper

import (
	"context"
	"encoding/json"
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

// areaWaterProduct defines the shapefile columns for area water features.
var areaWaterProduct = tiger.Product{
	Name:     "AREAWATER",
	Table:    "areawater",
	Columns:  []string{"ansicode", "hydroid", "fullname", "mtfcc", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

// linearWaterProduct defines the shapefile columns for linear water features.
var linearWaterProduct = tiger.Product{
	Name:     "LINEARWATER",
	Table:    "linearwater",
	Columns:  []string{"ansicode", "linearid", "fullname", "mtfcc"},
	GeomType: "MULTILINESTRING",
}

var waterCols = []string{
	"name", "water_type", "mtfcc",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

var waterConflictKeys = []string{"source", "source_id"}

// classifyWater maps a TIGER MTFCC water code to a human-readable type.
func classifyWater(mtfcc string) string {
	switch mtfcc {
	case "H2030", "H2040":
		return "lake"
	case "H2051", "H2053":
		return "reservoir"
	case "H3010", "H3013", "H3020":
		return "stream"
	case "H2025":
		return "swamp"
	case "H1100":
		return "ocean"
	default:
		return "water"
	}
}

// TIGERWater scrapes area and linear water features from Census TIGER/Line
// county-level shapefiles.
type TIGERWater struct {
	downloadBaseURL string   // override for testing; empty uses census.gov
	year            int      // override for testing; 0 uses tigerYear
	countyFIPS      []string // override for testing; empty uses DB lookup
}

// Name implements GeoScraper.
func (t *TIGERWater) Name() string { return "tiger_water" }

// Table implements GeoScraper.
func (t *TIGERWater) Table() string { return "geo.water_features" }

// Category implements GeoScraper.
func (t *TIGERWater) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (t *TIGERWater) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (t *TIGERWater) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (t *TIGERWater) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", t.Name()))
	log.Info("starting TIGER water features sync")

	year := tigerYear
	if t.year > 0 {
		year = t.year
	}

	counties := t.countyFIPS
	if len(counties) == 0 {
		var err error
		counties, err = allCountyFIPS(ctx, pool)
		if err != nil {
			return nil, eris.Wrap(err, "tiger_water: list county FIPS")
		}
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        t.Table(),
			Columns:      waterCols,
			ConflictKeys: waterConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "tiger_water: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	// Area water.
	for _, fips := range counties {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := t.buildURL(year, fips, "AREAWATER", "areawater")
		rows, err := t.loadWater(ctx, url, tempDir, areaWaterProduct, true)
		if err != nil {
			log.Debug("area water download failed, skipping county",
				zap.String("fips", fips), zap.Error(err))
			continue
		}
		batch = append(batch, rows...)
		if len(batch) >= tigerBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	// Linear water.
	for _, fips := range counties {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := t.buildURL(year, fips, "LINEARWATER", "linearwater")
		rows, err := t.loadWater(ctx, url, tempDir, linearWaterProduct, false)
		if err != nil {
			log.Debug("linear water download failed, skipping county",
				zap.String("fips", fips), zap.Error(err))
			continue
		}
		batch = append(batch, rows...)
		if len(batch) >= tigerBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("TIGER water features sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (t *TIGERWater) buildURL(year int, fips, dirName, fileKey string) string {
	filename := fmt.Sprintf("tl_%d_%s_%s.zip", year, fips, fileKey)
	suffix := fmt.Sprintf("%s/%s", dirName, filename)
	return tigerURL(t.downloadBaseURL, year, suffix)
}

func (t *TIGERWater) loadWater(ctx context.Context, url, tempDir string, product tiger.Product, isArea bool) ([][]any, error) {
	shpPath, err := tiger.Download(ctx, url, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "download water")
	}

	result, err := tiger.ParseShapefile(shpPath, product)
	if err != nil {
		return nil, eris.Wrap(err, "parse water shapefile")
	}
	result = filterToProductColumns(result, product)

	var rows [][]any
	for _, raw := range result.Rows {
		var row []any
		if isArea {
			row = newAreaWaterRow(raw)
		} else {
			row = newLinearWaterRow(raw)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func newAreaWaterRow(raw []any) []any {
	// raw: ansicode, hydroid, fullname, mtfcc, aland, awater, intptlat, intptlon, wkb
	fullname := strVal(raw, 2)
	mtfcc := strVal(raw, 3)
	lat, lon := parseLatLon(raw, 6, 7)
	hydroID := strVal(raw, 1)
	sourceID := fmt.Sprintf("tiger/aw/%s", hydroID)
	if hydroID == "" {
		sourceID = fmt.Sprintf("tiger/aw/%s", strVal(raw, 0))
	}
	props, _ := json.Marshal(map[string]any{
		"ansicode": strVal(raw, 0),
		"hydroid":  hydroID,
		"aland":    strVal(raw, 4),
		"awater":   strVal(raw, 5),
	})
	return []any{
		fullname,
		classifyWater(mtfcc),
		mtfcc,
		raw[8], // geom (WKB)
		lat, lon,
		tigerGeoSource,
		sourceID,
		props,
	}
}

func newLinearWaterRow(raw []any) []any {
	// raw: ansicode, linearid, fullname, mtfcc, wkb
	fullname := strVal(raw, 2)
	mtfcc := strVal(raw, 3)
	linearID := strVal(raw, 1)
	sourceID := fmt.Sprintf("tiger/lw/%s", linearID)
	if linearID == "" {
		sourceID = fmt.Sprintf("tiger/lw/%s", strVal(raw, 0))
	}
	props, _ := json.Marshal(map[string]any{
		"ansicode": strVal(raw, 0),
		"linearid": linearID,
	})
	return []any{
		fullname,
		classifyWater(mtfcc),
		mtfcc,
		raw[4],   // geom (WKB)
		0.0, 0.0, // linear water has no centroid in shapefile
		tigerGeoSource,
		sourceID,
		props,
	}
}

// allCountyFIPS queries geo.counties for all 5-digit county FIPS codes.
func allCountyFIPS(ctx context.Context, pool db.Pool) ([]string, error) {
	rows, err := pool.Query(ctx,
		"SELECT state_fips || county_fips FROM geo.counties ORDER BY state_fips, county_fips")
	if err != nil {
		return nil, eris.Wrap(err, "query county FIPS")
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var fips string
		if err := rows.Scan(&fips); err != nil {
			return nil, eris.Wrap(err, "scan county FIPS")
		}
		codes = append(codes, fips)
	}
	return codes, rows.Err()
}
