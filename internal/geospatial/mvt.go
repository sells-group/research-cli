package geospatial

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// LayerConfig defines how a database table maps to an MVT tile layer.
type LayerConfig struct {
	Table      string `json:"table"`
	GeomColumn string `json:"geom_column"`
	Columns    string `json:"columns"` // comma-separated columns to include
	IsPoint    bool   `json:"is_point"`
	MinZoom    int    `json:"min_zoom"`
	MaxZoom    int    `json:"max_zoom"`
}

// validMVTTables is an allowlist of table names that may appear in MVT generation queries.
var validMVTTables = map[string]bool{
	"geo.counties":       true,
	"geo.places":         true,
	"geo.cbsa":           true,
	"geo.poi":            true,
	"geo.infrastructure": true,
	"geo.epa_sites":      true,
	"geo.flood_zones":    true,
}

// DefaultLayers returns the standard layer configurations for MVT tile generation.
func DefaultLayers() map[string]LayerConfig {
	return map[string]LayerConfig{
		"counties": {
			Table:      "geo.counties",
			GeomColumn: "geom",
			Columns:    "id, geoid, name, state_fips, county_fips",
			MinZoom:    3,
			MaxZoom:    12,
		},
		"places": {
			Table:      "geo.places",
			GeomColumn: "geom",
			Columns:    "id, geoid, name, state_fips, lsad",
			MinZoom:    6,
			MaxZoom:    14,
		},
		"cbsa": {
			Table:      "geo.cbsa",
			GeomColumn: "geom",
			Columns:    "id, cbsa_code, name, lsad",
			MinZoom:    3,
			MaxZoom:    10,
		},
		"poi": {
			Table:      "geo.poi",
			GeomColumn: "geom",
			Columns:    "id, name, category, subcategory",
			IsPoint:    true,
			MinZoom:    8,
			MaxZoom:    16,
		},
		"infrastructure": {
			Table:      "geo.infrastructure",
			GeomColumn: "geom",
			Columns:    "id, name, type, fuel_type, capacity",
			IsPoint:    true,
			MinZoom:    6,
			MaxZoom:    14,
		},
		"epa_sites": {
			Table:      "geo.epa_sites",
			GeomColumn: "geom",
			Columns:    "id, name, program, status",
			IsPoint:    true,
			MinZoom:    8,
			MaxZoom:    16,
		},
		"flood_zones": {
			Table:      "geo.flood_zones",
			GeomColumn: "geom",
			Columns:    "id, zone_code, flood_type",
			MinZoom:    8,
			MaxZoom:    16,
		},
	}
}

// GenerateMVT generates a Mapbox Vector Tile for the given layer and tile coordinates.
// The layer's table must be in the validMVTTables allowlist.
func GenerateMVT(ctx context.Context, pool db.Pool, layer LayerConfig, z, x, y int) ([]byte, error) {
	if !validMVTTables[layer.Table] {
		return nil, eris.Errorf("geo: invalid MVT table %q", layer.Table)
	}

	sql := fmt.Sprintf(`
		SELECT ST_AsMVT(q, 'default', 4096, 'geom') FROM (
			SELECT %s,
				ST_AsMVTGeom(
					%s,
					ST_TileEnvelope($1, $2, $3),
					4096, 256, true
				) AS geom
			FROM %s
			WHERE %s && ST_TileEnvelope($1, $2, $3)
		) q`,
		layer.Columns,
		layer.GeomColumn,
		layer.Table,
		layer.GeomColumn,
	)

	var tile []byte
	err := pool.QueryRow(ctx, sql, z, x, y).Scan(&tile)
	if err != nil {
		return nil, eris.Wrap(err, "geo: generate MVT")
	}
	return tile, nil
}
