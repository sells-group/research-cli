package geospatial

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// TableStats holds size and row count information for a geo table.
type TableStats struct {
	TableName  string `json:"table_name"`
	RowCount   int64  `json:"row_count"`
	TotalSize  string `json:"total_size"`
	IndexSize  string `json:"index_size"`
	HasSpatial bool   `json:"has_spatial"`
}

// geoTables lists all tables in the geo schema that maintenance commands operate on.
var geoTables = []string{
	"geo.counties",
	"geo.places",
	"geo.zcta",
	"geo.cbsa",
	"geo.census_tracts",
	"geo.congressional_districts",
	"geo.poi",
	"geo.infrastructure",
	"geo.epa_sites",
	"geo.flood_zones",
	"geo.demographics",
	"geo.geocode_cache",
	"geo.geocode_queue",
}

// VacuumAnalyze runs VACUUM ANALYZE on all geo.* tables to update planner
// statistics and reclaim dead tuple space.
func VacuumAnalyze(ctx context.Context, pool db.Pool) error {
	for _, table := range geoTables {
		sql := fmt.Sprintf("VACUUM ANALYZE %s", table)
		zap.L().Info("geo: vacuum analyze", zap.String("table", table))
		if _, err := pool.Exec(ctx, sql); err != nil {
			return eris.Wrapf(err, "geo: vacuum analyze %s", table)
		}
	}
	return nil
}

// ClusterSpatialIndexes runs CLUSTER on spatial tables to physically reorder
// rows by their GIST index, improving spatial query performance.
func ClusterSpatialIndexes(ctx context.Context, pool db.Pool) error {
	spatialIndexes := map[string]string{
		"geo.counties":                "idx_counties_geom",
		"geo.places":                  "idx_places_geom",
		"geo.zcta":                    "idx_zcta_geom",
		"geo.cbsa":                    "idx_cbsa_geom",
		"geo.census_tracts":           "idx_census_tracts_geom",
		"geo.congressional_districts": "idx_congressional_districts_geom",
		"geo.poi":                     "idx_poi_geom",
		"geo.infrastructure":          "idx_infrastructure_geom",
		"geo.epa_sites":               "idx_epa_sites_geom",
		"geo.flood_zones":             "idx_flood_zones_geom",
		"geo.demographics":            "idx_demographics_geom",
	}
	for table, index := range spatialIndexes {
		sql := fmt.Sprintf("CLUSTER %s USING %s", table, index)
		zap.L().Info("geo: cluster", zap.String("table", table), zap.String("index", index))
		if _, err := pool.Exec(ctx, sql); err != nil {
			return eris.Wrapf(err, "geo: cluster %s using %s", table, index)
		}
	}
	return nil
}

// GetTableStats returns size and row count statistics for all geo.* tables.
func GetTableStats(ctx context.Context, pool db.Pool) ([]TableStats, error) {
	sql := `
		SELECT
			schemaname || '.' || relname AS table_name,
			n_live_tup AS row_count,
			pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size,
			pg_size_pretty(pg_indexes_size(schemaname || '.' || relname)) AS index_size,
			EXISTS (
				SELECT 1 FROM pg_indexes
				WHERE schemaname = s.schemaname AND tablename = s.relname
				AND indexdef LIKE '%USING gist%'
			) AS has_spatial
		FROM pg_stat_user_tables s
		WHERE schemaname = 'geo'
		ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
	`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query table stats")
	}
	defer rows.Close()

	var stats []TableStats
	for rows.Next() {
		var s TableStats
		if err := rows.Scan(&s.TableName, &s.RowCount, &s.TotalSize, &s.IndexSize, &s.HasSpatial); err != nil {
			return nil, eris.Wrap(err, "geo: scan table stats row")
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate table stats rows")
	}
	return stats, nil
}

// ReindexSpatial rebuilds all spatial (GIST) indexes in the geo schema.
func ReindexSpatial(ctx context.Context, pool db.Pool) error {
	zap.L().Info("geo: reindexing spatial indexes")
	_, err := pool.Exec(ctx, "REINDEX SCHEMA geo")
	if err != nil {
		return eris.Wrap(err, "geo: reindex schema")
	}
	return nil
}
