package scraper

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// femaSource is the source identifier for all FEMA scrapers.
const femaSource = "fema"

// femaBatchSize is the number of rows per upsert batch.
const femaBatchSize = 5000

// femaBaseURL is the FEMA NFHL Flood Hazard Areas FeatureServer endpoint.
const femaBaseURL = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Flood_Hazard_Reduced_Set/FeatureServer/0/query"

// femaURL returns the base URL, falling back to the default FEMA endpoint
// if override is empty. The override is used for testing.
func femaURL(override string) string {
	if override != "" {
		return override
	}
	return femaBaseURL
}

// stateFIPS lists all 50 US states + DC + territories by 2-digit FIPS code.
var stateFIPS = []string{
	"01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
	"13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
	"24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
	"34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
	"45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
	"56",
	// Territories
	"60", "66", "69", "72", "78",
}

// femaFloodType derives the flood type classification from a FEMA zone code,
// SFHA_TF indicator, and zone subtype.
//
// High risk: SFHA_TF=T (zones A, AE, AH, AO, AR, A99, V, VE)
// Moderate risk: zone X with "0.2 PCT" subtype (shaded X / X500)
// Low risk: zone X without "0.2 PCT" subtype (unshaded X)
// Undetermined: zone D
func femaFloodType(zoneCode, sfhaTF, zoneSubtype string) string {
	zone := strings.TrimSpace(strings.ToUpper(zoneCode))
	sfha := strings.TrimSpace(strings.ToUpper(sfhaTF))

	if zone == "D" {
		return "undetermined"
	}

	if sfha == "T" {
		return "high_risk"
	}

	if zone == "X" && strings.Contains(strings.ToUpper(zoneSubtype), "0.2 PCT") {
		return "moderate_risk"
	}

	return "low_risk"
}

// floodCols are the columns written to the temp table for flood zone upserts.
// geom_wkt is TEXT in the temp table; converted to geometry via ST_GeomFromEWKT.
var floodCols = []string{
	"zone_code", "flood_type", "geom_wkt",
	"source", "source_id", "properties",
}

// floodExclude lists attribute keys stored in dedicated columns.
var floodExclude = map[string]bool{
	"OBJECTID":   true,
	"FLD_ZONE":   true,
	"FLD_AR_ID":  true,
	"SFHA_TF":    true,
	"ZONE_SUBTY": true,
}

// floodUpsert performs a custom upsert for flood zone rows that converts EWKT
// geometry via ST_GeomFromEWKT during the INSERT step.
func floodUpsert(ctx context.Context, pool db.Pool, table string, batch [][]any) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, eris.Wrap(err, "flood_zones: begin tx")
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is no-op

	// Create temp table with explicit columns (geom_wkt TEXT instead of geom GEOMETRY).
	createSQL := `CREATE TEMP TABLE _tmp_flood_zones (
		zone_code  TEXT,
		flood_type TEXT,
		geom_wkt   TEXT,
		source     TEXT,
		source_id  TEXT,
		properties JSONB
	) ON COMMIT DROP`
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return 0, eris.Wrap(err, "flood_zones: create temp table")
	}

	// COPY rows into temp table.
	copySource := pgx.CopyFromRows(batch)
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"_tmp_flood_zones"}, floodCols, copySource); err != nil {
		return 0, eris.Wrap(err, "flood_zones: COPY into temp table")
	}

	// Deduplicate by (source, source_id), keeping last row.
	dedupSQL := `DELETE FROM _tmp_flood_zones a USING _tmp_flood_zones b
		WHERE a.ctid < b.ctid AND a.source = b.source AND a.source_id = b.source_id`
	if _, err := tx.Exec(ctx, dedupSQL); err != nil {
		return 0, eris.Wrap(err, "flood_zones: dedup temp table")
	}

	// INSERT with ST_GeomFromEWKT conversion.
	upsertSQL := fmt.Sprintf(
		`INSERT INTO %s (zone_code, flood_type, geom, source, source_id, properties)
		 SELECT zone_code, flood_type, ST_GeomFromEWKT(geom_wkt), source, source_id, properties
		 FROM _tmp_flood_zones
		 ON CONFLICT (source, source_id) DO UPDATE SET
			zone_code  = EXCLUDED.zone_code,
			flood_type = EXCLUDED.flood_type,
			geom       = EXCLUDED.geom,
			properties = EXCLUDED.properties,
			updated_at = now()`,
		sanitizeGeoTable(table),
	)

	tag, err := tx.Exec(ctx, upsertSQL)
	if err != nil {
		return 0, eris.Wrap(err, "flood_zones: INSERT ON CONFLICT")
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, eris.Wrap(err, "flood_zones: commit tx")
	}

	return tag.RowsAffected(), nil
}

// sanitizeGeoTable handles schema-qualified table names like "geo.flood_zones".
func sanitizeGeoTable(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return pgx.Identifier{parts[0], parts[1]}.Sanitize()
	}
	return pgx.Identifier{table}.Sanitize()
}

// buildFEMAWhere returns an ArcGIS WHERE clause to filter by state FIPS code.
func buildFEMAWhere(stateFIPSCode string) string {
	return fmt.Sprintf("DFIRM_ID LIKE '%s%%'", stateFIPSCode)
}

// newFloodRow builds a row for the flood zone temp table from an ArcGIS feature.
// Returns nil, false if the feature has no geometry or no rings.
func newFloodRow(feat arcgis.Feature) ([]any, bool) {
	if feat.Geometry == nil || len(feat.Geometry.Rings) == 0 {
		return nil, false
	}

	zoneCode := hifldString(feat.Attributes, "FLD_ZONE")
	sfhaTF := hifldString(feat.Attributes, "SFHA_TF")
	zoneSubtype := hifldString(feat.Attributes, "ZONE_SUBTY")
	floodType := femaFloodType(zoneCode, sfhaTF, zoneSubtype)
	geomWKT := feat.Geometry.RingsToEWKT()
	sourceID := fmt.Sprintf("%v", feat.Attributes["FLD_AR_ID"])

	return []any{
		zoneCode,
		floodType,
		geomWKT,
		femaSource,
		sourceID,
		hifldProperties(feat.Attributes, floodExclude),
	}, true
}
