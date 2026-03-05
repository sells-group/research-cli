package scraper

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/sells-group/research-cli/internal/tiger"
)

// tigerGeoSource is the source identifier for all TIGER geo scrapers.
const tigerGeoSource = "tiger"

// tigerBatchSize is the number of rows per BulkUpsert batch.
const tigerBatchSize = 5000

// tigerYear is the default TIGER/Line vintage year.
const tigerYear = 2024

// tigerBaseURL is the Census TIGER/Line download root.
const tigerBaseURL = "https://www2.census.gov/geo/tiger/TIGER%d"

// boundaryDef defines a single TIGER boundary type for the boundaries scraper.
type boundaryDef struct {
	name        string // for logging
	table       string // target table (e.g., "geo.counties")
	product     tiger.Product
	conflictKey string // single natural key column for ON CONFLICT
	national    bool   // true = single national file, false = per-state
	columns     []string
	buildRow    func(raw []any) []any
}

// tigerURL builds a download URL with an optional base override for testing.
func tigerURL(baseOverride string, year int, pathSuffix string) string {
	base := baseOverride
	if base == "" {
		base = fmt.Sprintf(tigerBaseURL, year)
	}
	return base + "/" + pathSuffix
}

// boundaryDefs returns the six boundary definitions for the TIGER boundaries scraper.
func boundaryDefs() []boundaryDef {
	return []boundaryDef{
		countyDef(),
		placeDef(),
		zctaDef(),
		cbsaDef(),
		censusTractDef(),
		congressionalDistrictDef(),
	}
}

// --- County ---

var countyProduct = tiger.Product{
	Name:     "COUNTY",
	Table:    "county",
	Columns:  []string{"statefp", "countyfp", "geoid", "name", "namelsad", "lsad", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var countyCols = []string{
	"geoid", "state_fips", "county_fips", "name", "lsad",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func countyDef() boundaryDef {
	return boundaryDef{
		name:        "counties",
		table:       "geo.counties",
		product:     countyProduct,
		conflictKey: "geoid",
		national:    true,
		columns:     countyCols,
		buildRow:    newCountyRow,
	}
}

func newCountyRow(raw []any) []any {
	// raw: statefp, countyfp, geoid, name, namelsad, lsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 2)
	lat, lon := parseLatLon(raw, 10, 11)
	props := boundaryProperties(raw,
		"namelsad", strVal(raw, 4),
		"mtfcc", strVal(raw, 6),
		"funcstat", strVal(raw, 7),
		"aland", strVal(raw, 8),
		"awater", strVal(raw, 9),
	)
	return []any{
		geoid,
		strVal(raw, 0), // state_fips
		strVal(raw, 1), // county_fips
		strVal(raw, 3), // name
		strVal(raw, 5), // lsad
		raw[12],        // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}

// --- Place ---

var placeProduct = tiger.Product{
	Name:     "PLACE",
	Table:    "place",
	Columns:  []string{"statefp", "placefp", "geoid", "name", "namelsad", "lsad", "classfp", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var placeCols = []string{
	"geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func placeDef() boundaryDef {
	return boundaryDef{
		name:        "places",
		table:       "geo.places",
		product:     placeProduct,
		conflictKey: "geoid",
		national:    false,
		columns:     placeCols,
		buildRow:    newPlaceRow,
	}
}

func newPlaceRow(raw []any) []any {
	// raw: statefp, placefp, geoid, name, namelsad, lsad, classfp, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 2)
	lat, lon := parseLatLon(raw, 11, 12)
	props := boundaryProperties(raw,
		"namelsad", strVal(raw, 4),
		"mtfcc", strVal(raw, 7),
		"funcstat", strVal(raw, 8),
		"aland", strVal(raw, 9),
		"awater", strVal(raw, 10),
	)
	return []any{
		geoid,
		strVal(raw, 0), // state_fips
		strVal(raw, 1), // place_fips
		strVal(raw, 3), // name
		strVal(raw, 5), // lsad
		strVal(raw, 6), // class_fips
		raw[13],        // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}

// --- ZCTA ---

var zctaProduct = tiger.Product{
	Name:     "ZCTA520",
	Table:    "zcta520",
	Columns:  []string{"zcta5ce20", "geoid20", "classfp20", "mtfcc20", "funcstat20", "aland20", "awater20", "intptlat20", "intptlon20"},
	GeomType: "MULTIPOLYGON",
}

var zctaCols = []string{
	"zcta5", "state_fips", "aland", "awater",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func zctaDef() boundaryDef {
	return boundaryDef{
		name:        "zcta",
		table:       "geo.zcta",
		product:     zctaProduct,
		conflictKey: "zcta5",
		national:    true,
		columns:     zctaCols,
		buildRow:    newZCTARow,
	}
}

func newZCTARow(raw []any) []any {
	// raw: zcta5ce20, geoid20, classfp20, mtfcc20, funcstat20, aland20, awater20, intptlat20, intptlon20, wkb
	zcta5 := strVal(raw, 0)
	lat, lon := parseLatLon(raw, 7, 8)
	// Derive state FIPS from first 2 digits of geoid20 if available.
	stateFIPS := ""
	if g := strVal(raw, 1); len(g) >= 2 {
		stateFIPS = g[:2]
	}
	aland := parseInt64Val(raw, 5)
	awater := parseInt64Val(raw, 6)
	props := boundaryProperties(raw,
		"geoid20", strVal(raw, 1),
		"classfp20", strVal(raw, 2),
		"mtfcc20", strVal(raw, 3),
		"funcstat20", strVal(raw, 4),
	)
	return []any{
		zcta5,
		stateFIPS,
		aland, awater,
		raw[9], // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", zcta5),
		props,
	}
}

// --- CBSA ---

var cbsaProduct = tiger.Product{
	Name:     "CBSA",
	Table:    "cbsa",
	Columns:  []string{"cbsafp", "name", "namelsad", "lsad", "mtfcc", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var cbsaCols = []string{
	"cbsa_code", "name", "lsad",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func cbsaDef() boundaryDef {
	return boundaryDef{
		name:        "cbsa",
		table:       "geo.cbsa",
		product:     cbsaProduct,
		conflictKey: "cbsa_code",
		national:    true,
		columns:     cbsaCols,
		buildRow:    newCBSARow,
	}
}

func newCBSARow(raw []any) []any {
	// raw: cbsafp, name, namelsad, lsad, mtfcc, aland, awater, intptlat, intptlon, wkb
	cbsaCode := strVal(raw, 0)
	lat, lon := parseLatLon(raw, 7, 8)
	props := boundaryProperties(raw,
		"namelsad", strVal(raw, 2),
		"mtfcc", strVal(raw, 4),
		"aland", strVal(raw, 5),
		"awater", strVal(raw, 6),
	)
	return []any{
		cbsaCode,
		strVal(raw, 1), // name
		strVal(raw, 3), // lsad
		raw[9],         // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", cbsaCode),
		props,
	}
}

// --- Census Tracts ---

var censusTractProduct = tiger.Product{
	Name:     "TRACT",
	Table:    "tract",
	Columns:  []string{"statefp", "countyfp", "tractce", "geoid", "name", "namelsad", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var censusTractCols = []string{
	"geoid", "state_fips", "county_fips", "tract_ce", "name",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func censusTractDef() boundaryDef {
	return boundaryDef{
		name:        "census_tracts",
		table:       "geo.census_tracts",
		product:     censusTractProduct,
		conflictKey: "geoid",
		national:    false,
		columns:     censusTractCols,
		buildRow:    newCensusTractRow,
	}
}

func newCensusTractRow(raw []any) []any {
	// raw: statefp, countyfp, tractce, geoid, name, namelsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 3)
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
		strVal(raw, 0), // state_fips
		strVal(raw, 1), // county_fips
		strVal(raw, 2), // tract_ce
		strVal(raw, 4), // name
		raw[12],        // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}

// --- Congressional Districts ---

var congressionalDistrictProduct = tiger.Product{
	Name:     "CD",
	Table:    "cd119",
	Columns:  []string{"statefp", "cd119fp", "geoid", "namelsad", "lsad", "mtfcc", "funcstat", "aland", "awater", "intptlat", "intptlon"},
	GeomType: "MULTIPOLYGON",
}

var congressionalDistrictCols = []string{
	"geoid", "state_fips", "district", "congress", "name", "lsad",
	"geom", "latitude", "longitude",
	"source", "source_id", "properties",
}

func congressionalDistrictDef() boundaryDef {
	return boundaryDef{
		name:        "congressional_districts",
		table:       "geo.congressional_districts",
		product:     congressionalDistrictProduct,
		conflictKey: "geoid",
		national:    false,
		columns:     congressionalDistrictCols,
		buildRow:    newCongressionalDistrictRow,
	}
}

func newCongressionalDistrictRow(raw []any) []any {
	// raw: statefp, cd119fp, geoid, namelsad, lsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, wkb
	geoid := strVal(raw, 2)
	lat, lon := parseLatLon(raw, 9, 10)
	props := boundaryProperties(raw,
		"mtfcc", strVal(raw, 5),
		"funcstat", strVal(raw, 6),
		"aland", strVal(raw, 7),
		"awater", strVal(raw, 8),
	)
	return []any{
		geoid,
		strVal(raw, 0), // state_fips
		strVal(raw, 1), // district
		"119",          // congress (119th)
		strVal(raw, 3), // name (namelsad)
		strVal(raw, 4), // lsad
		raw[11],        // geom (WKB)
		lat, lon,
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", geoid),
		props,
	}
}

// --- Road helpers ---

// tigerRoadProduct defines the shapefile columns for TIGER primary roads.
var tigerRoadProduct = tiger.Product{
	Name:     "PRIMARYROADS",
	Table:    "primaryroads",
	Columns:  []string{"fullname", "mtfcc", "linearid"},
	GeomType: "MULTILINESTRING",
}

// roadCols are the columns written to geo.roads by the roads scraper.
var roadCols = []string{
	"name", "route_type", "mtfcc", "geom", "source", "source_id", "properties",
}

// roadConflictKeys defines the unique constraint columns for road upserts.
var roadConflictKeys = []string{"source", "source_id"}

// classifyRoad maps a TIGER MTFCC code to a human-readable route type.
func classifyRoad(mtfcc string) string {
	switch mtfcc {
	case "S1100":
		return "interstate"
	case "S1200":
		return "us_highway"
	case "S1300":
		return "state_highway"
	default:
		return "local"
	}
}

// newRoadRow builds a row for the roads table from a parsed shapefile row.
func newRoadRow(raw []any) []any {
	// raw: fullname, mtfcc, linearid, wkb
	fullname := strVal(raw, 0)
	mtfcc := strVal(raw, 1)
	linearID := strVal(raw, 2)
	props := boundaryProperties(raw,
		"linearid", linearID,
	)
	return []any{
		fullname,
		classifyRoad(mtfcc),
		mtfcc,
		raw[3], // geom (WKB)
		tigerGeoSource,
		fmt.Sprintf("tiger/%s", linearID),
		props,
	}
}

// --- Shared helpers ---

// strVal safely extracts a string from a parsed shapefile row.
func strVal(raw []any, idx int) string {
	if idx >= len(raw) || raw[idx] == nil {
		return ""
	}
	s, ok := raw[idx].(string)
	if !ok {
		return fmt.Sprintf("%v", raw[idx])
	}
	return s
}

// parseLatLon parses latitude and longitude strings from a shapefile row.
func parseLatLon(raw []any, latIdx, lonIdx int) (float64, float64) {
	lat, _ := strconv.ParseFloat(strVal(raw, latIdx), 64)
	lon, _ := strconv.ParseFloat(strVal(raw, lonIdx), 64)
	return lat, lon
}

// parseInt64Val parses a string value from a shapefile row as int64.
func parseInt64Val(raw []any, idx int) *int64 {
	s := strVal(raw, idx)
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// boundaryProperties builds a JSONB-compatible byte slice from key-value pairs,
// excluding empty values.
func boundaryProperties(_ []any, kvPairs ...string) []byte {
	props := make(map[string]string)
	for i := 0; i+1 < len(kvPairs); i += 2 {
		if kvPairs[i+1] != "" {
			props[kvPairs[i]] = kvPairs[i+1]
		}
	}
	data, err := json.Marshal(props)
	if err != nil {
		return []byte("{}")
	}
	return data
}
