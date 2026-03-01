package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// censusSource is the source identifier for Census ACS scrapers.
const censusSource = "census"

// censusBatchSize is the number of rows per upsert batch.
const censusBatchSize = 5000

// censusGeoLevel is the geography level for tract-level data.
const censusGeoLevel = "tract"

// censusACSYear is the default ACS 5-year data vintage.
const censusACSYear = 2023

// defaultACSBaseURL is the Census ACS 5-year API endpoint.
const defaultACSBaseURL = "https://api.census.gov/data/%d/acs/acs5"

// defaultTigerBaseURL is the TIGERweb ArcGIS REST endpoint for census tracts.
const defaultTigerBaseURL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/8/query"

// acsVars lists the ACS 5-year variables to fetch.
var acsVars = []string{
	"B01003_001E", // total population
	"B19013_001E", // median household income
	"B01002_001E", // median age
	"B25001_001E", // housing units
}

// demoCols are the columns written to the temp table for demographics upserts.
// geom_wkt is TEXT in the temp table; converted to geometry via ST_GeomFromEWKT.
var demoCols = []string{
	"geoid", "geo_level", "year",
	"total_population", "median_income", "median_age", "housing_units",
	"geom_wkt",
	"source", "source_id", "properties",
}

// demoRow holds parsed Census ACS demographic values for a single tract.
type demoRow struct {
	geoid      string
	population *int
	income     *float64
	age        *float64
	housing    *int
}

// parseACSResponse parses the Census API JSON response ([][]string format) into
// a map of GEOID → demoRow. The first row is the header; subsequent rows are data.
func parseACSResponse(data []byte) (map[string]*demoRow, error) {
	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, eris.Wrap(err, "census: parse ACS response")
	}
	if len(rows) < 2 {
		return nil, nil
	}

	// Build header index.
	header := rows[0]
	idx := make(map[string]int, len(header))
	for i, col := range header {
		idx[col] = i
	}

	result := make(map[string]*demoRow, len(rows)-1)
	for _, row := range rows[1:] {
		stateCol := safeIndex(row, idx, "state")
		countyCol := safeIndex(row, idx, "county")
		tractCol := safeIndex(row, idx, "tract")
		if stateCol == "" || countyCol == "" || tractCol == "" {
			continue
		}
		geoid := stateCol + countyCol + tractCol

		dr := &demoRow{geoid: geoid}
		if v := safeIndex(row, idx, "B01003_001E"); v != "" {
			n := parseIntOrNil(v)
			dr.population = n
		}
		if v := safeIndex(row, idx, "B19013_001E"); v != "" {
			f := parseFloatOrNil(v)
			dr.income = f
		}
		if v := safeIndex(row, idx, "B01002_001E"); v != "" {
			f := parseFloatOrNil(v)
			dr.age = f
		}
		if v := safeIndex(row, idx, "B25001_001E"); v != "" {
			n := parseIntOrNil(v)
			dr.housing = n
		}
		result[geoid] = dr
	}

	return result, nil
}

// safeIndex returns the value at the given column name, or "" if missing.
func safeIndex(row []string, idx map[string]int, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return ""
	}
	return row[i]
}

// parseIntOrNil converts a string to *int, returning nil for empty/invalid values.
func parseIntOrNil(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" || s == "-666666666" || s == "null" {
		return nil
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return nil
	}
	return &n
}

// parseFloatOrNil converts a string to *float64, returning nil for empty/invalid values.
func parseFloatOrNil(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-666666666" || s == "-666666666.0" || s == "null" {
		return nil
	}
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err != nil {
		return nil
	}
	return &f
}

// buildACSURL constructs the Census ACS API URL for a given state FIPS.
// The baseURL already includes the year (e.g., ".../2023/acs/acs5").
func buildACSURL(baseURL, apiKey, fips string) string {
	vars := strings.Join(acsVars, ",")
	u := fmt.Sprintf("%s?get=%s&for=tract:*&in=state:%s&in=county:*", baseURL, vars, fips)
	if apiKey != "" {
		u += "&key=" + apiKey
	}
	return u
}

// demoUpsert performs a custom upsert for demographics rows that converts EWKT
// geometry via ST_GeomFromEWKT during the INSERT step.
func demoUpsert(ctx context.Context, pool db.Pool, table string, batch [][]any) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, eris.Wrap(err, "demographics: begin tx")
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is no-op

	// Create temp table with explicit columns (geom_wkt TEXT instead of geom GEOMETRY).
	createSQL := `CREATE TEMP TABLE _tmp_demographics (
		geoid            TEXT,
		geo_level        TEXT,
		year             INTEGER,
		total_population INTEGER,
		median_income    DOUBLE PRECISION,
		median_age       DOUBLE PRECISION,
		housing_units    INTEGER,
		geom_wkt         TEXT,
		source           TEXT,
		source_id        TEXT,
		properties       JSONB
	) ON COMMIT DROP`
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return 0, eris.Wrap(err, "demographics: create temp table")
	}

	// COPY rows into temp table.
	copySource := pgx.CopyFromRows(batch)
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"_tmp_demographics"}, demoCols, copySource); err != nil {
		return 0, eris.Wrap(err, "demographics: COPY into temp table")
	}

	// Deduplicate by (geoid, geo_level, year), keeping last row.
	dedupSQL := `DELETE FROM _tmp_demographics a USING _tmp_demographics b
		WHERE a.ctid < b.ctid AND a.geoid = b.geoid AND a.geo_level = b.geo_level AND a.year = b.year`
	if _, err := tx.Exec(ctx, dedupSQL); err != nil {
		return 0, eris.Wrap(err, "demographics: dedup temp table")
	}

	// INSERT with ST_GeomFromEWKT conversion.
	upsertSQL := fmt.Sprintf(
		`INSERT INTO %s (geoid, geo_level, year, total_population, median_income, median_age, housing_units, geom, source, source_id, properties)
		 SELECT geoid, geo_level, year, total_population, median_income, median_age, housing_units,
		        ST_GeomFromEWKT(geom_wkt), source, source_id, properties
		 FROM _tmp_demographics
		 ON CONFLICT (geoid, geo_level, year) DO UPDATE SET
			total_population = EXCLUDED.total_population,
			median_income    = EXCLUDED.median_income,
			median_age       = EXCLUDED.median_age,
			housing_units    = EXCLUDED.housing_units,
			geom             = EXCLUDED.geom,
			source           = EXCLUDED.source,
			source_id        = EXCLUDED.source_id,
			properties       = EXCLUDED.properties,
			updated_at       = now()`,
		sanitizeGeoTable(table),
	)

	tag, err := tx.Exec(ctx, upsertSQL)
	if err != nil {
		return 0, eris.Wrap(err, "demographics: INSERT ON CONFLICT")
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, eris.Wrap(err, "demographics: commit tx")
	}

	return tag.RowsAffected(), nil
}
