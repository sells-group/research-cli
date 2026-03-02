package geospatial

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// validTables is an allowlist of table names that may be passed to generic spatial
// query functions. This prevents SQL injection through the table parameter.
var validTables = map[string]bool{
	"geo.counties":                true,
	"geo.places":                  true,
	"geo.zcta":                    true,
	"geo.cbsa":                    true,
	"geo.census_tracts":           true,
	"geo.congressional_districts": true,
	"geo.poi":                     true,
	"geo.infrastructure":          true,
	"geo.epa_sites":               true,
	"geo.flood_zones":             true,
	"geo.demographics":            true,
}

// BBox represents a geographic bounding box.
type BBox struct {
	MinLng float64 `json:"min_lng"`
	MinLat float64 `json:"min_lat"`
	MaxLng float64 `json:"max_lng"`
	MaxLat float64 `json:"max_lat"`
}

// LocationContext holds the geographic context for a point.
type LocationContext struct {
	County                *County                `json:"county,omitempty"`
	Place                 *Place                 `json:"place,omitempty"`
	CBSA                  *CBSA                  `json:"cbsa,omitempty"`
	CensusTract           *CensusTract           `json:"census_tract,omitempty"`
	ZCTA                  *ZCTA                  `json:"zcta,omitempty"`
	CongressionalDistrict *CongressionalDistrict `json:"congressional_district,omitempty"`
}

// validateTable checks that the given table name is in the allowlist.
func validateTable(table string) error {
	if !validTables[table] {
		return eris.Errorf("geo: invalid table name %q", table)
	}
	return nil
}

// QueryWithinDistance finds entities within a given distance of a point.
// The table parameter must be one of the allowlisted geo.* table names.
// Results are ordered by proximity and capped at limit.
func QueryWithinDistance(ctx context.Context, pool db.Pool, table string, lng, lat, meters float64, limit int) (pgx.Rows, error) {
	if err := validateTable(table); err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(
		`SELECT * FROM %s WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3) ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326) LIMIT $4`,
		table,
	)
	rows, err := pool.Query(ctx, sql, lng, lat, meters, limit)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query within distance")
	}
	return rows, nil
}

// QueryBBox finds entities within a bounding box.
// The table parameter must be one of the allowlisted geo.* table names.
// Results are ordered by id with pagination support.
func QueryBBox(ctx context.Context, pool db.Pool, table string, bbox BBox, limit, offset int) (pgx.Rows, error) {
	if err := validateTable(table); err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(
		`SELECT * FROM %s WHERE geom && ST_MakeEnvelope($1, $2, $3, $4, 4326) ORDER BY id LIMIT $5 OFFSET $6`,
		table,
	)
	rows, err := pool.Query(ctx, sql, bbox.MinLng, bbox.MinLat, bbox.MaxLng, bbox.MaxLat, limit, offset)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query bbox")
	}
	return rows, nil
}

// SearchText searches POI by name using full-text search.
// Results are ranked by relevance and capped at limit.
func SearchText(ctx context.Context, pool db.Pool, query string, limit int) ([]POI, error) {
	sql := `
		SELECT id, name, category, subcategory, address, latitude, longitude,
		       source, source_id, properties, created_at, updated_at
		FROM geo.poi
		WHERE to_tsvector('english', name) @@ plainto_tsquery('english', $1)
		ORDER BY ts_rank(to_tsvector('english', name), plainto_tsquery('english', $1)) DESC
		LIMIT $2
	`
	rows, err := pool.Query(ctx, sql, query, limit)
	if err != nil {
		return nil, eris.Wrap(err, "geo: search text")
	}
	defer rows.Close()

	var pois []POI
	for rows.Next() {
		var p POI
		if err := rows.Scan(
			&p.ID, &p.Name, &p.Category, &p.Subcategory, &p.Address,
			&p.Latitude, &p.Longitude, &p.Source, &p.SourceID, &p.Properties,
			&p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return nil, eris.Wrap(err, "geo: scan text search row")
		}
		pois = append(pois, p)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate text search rows")
	}
	return pois, nil
}

// PointInPolygon returns the geographic context for a given lat/lng.
// It queries multiple geo tables to find which polygons contain the point.
// Fields that have no matching polygon are left nil.
func PointInPolygon(ctx context.Context, pool db.Pool, lng, lat float64) (*LocationContext, error) {
	lc := &LocationContext{}

	// County lookup.
	county, err := pipCounty(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.County = county

	// Place lookup.
	place, err := pipPlace(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.Place = place

	// CBSA lookup.
	cbsa, err := pipCBSA(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.CBSA = cbsa

	// Census tract lookup.
	tract, err := pipCensusTract(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.CensusTract = tract

	// ZCTA lookup.
	zcta, err := pipZCTA(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.ZCTA = zcta

	// Congressional district lookup.
	cd, err := pipCongressionalDistrict(ctx, pool, lng, lat)
	if err != nil {
		return nil, err
	}
	lc.CongressionalDistrict = cd

	return lc, nil
}

func pipCounty(ctx context.Context, pool db.Pool, lng, lat float64) (*County, error) {
	sql := `
		SELECT id, geoid, state_fips, county_fips, name, lsad,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.counties
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var c County
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&c.ID, &c.GEOID, &c.StateFIPS, &c.CountyFIPS, &c.Name, &c.LSAD,
		&c.Latitude, &c.Longitude, &c.Source, &c.SourceID, &c.Properties,
		&c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip county")
	}
	return &c, nil
}

func pipPlace(ctx context.Context, pool db.Pool, lng, lat float64) (*Place, error) {
	sql := `
		SELECT id, geoid, state_fips, place_fips, name, lsad, class_fips,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.places
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var p Place
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&p.ID, &p.GEOID, &p.StateFIPS, &p.PlaceFIPS, &p.Name, &p.LSAD, &p.ClassFIPS,
		&p.Latitude, &p.Longitude, &p.Source, &p.SourceID, &p.Properties,
		&p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip place")
	}
	return &p, nil
}

func pipCBSA(ctx context.Context, pool db.Pool, lng, lat float64) (*CBSA, error) {
	sql := `
		SELECT id, cbsa_code, name, lsad,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.cbsa
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var c CBSA
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&c.ID, &c.CBSACode, &c.Name, &c.LSAD,
		&c.Latitude, &c.Longitude, &c.Source, &c.SourceID, &c.Properties,
		&c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip cbsa")
	}
	return &c, nil
}

func pipCensusTract(ctx context.Context, pool db.Pool, lng, lat float64) (*CensusTract, error) {
	sql := `
		SELECT id, geoid, state_fips, county_fips, tract_ce, name,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.census_tracts
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var ct CensusTract
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&ct.ID, &ct.GEOID, &ct.StateFIPS, &ct.CountyFIPS, &ct.TractCE, &ct.Name,
		&ct.Latitude, &ct.Longitude, &ct.Source, &ct.SourceID, &ct.Properties,
		&ct.CreatedAt, &ct.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip census tract")
	}
	return &ct, nil
}

func pipZCTA(ctx context.Context, pool db.Pool, lng, lat float64) (*ZCTA, error) {
	sql := `
		SELECT id, zcta5, state_fips, aland, awater,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.zcta
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var z ZCTA
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&z.ID, &z.ZCTA5, &z.StateFIPS, &z.ALand, &z.AWater,
		&z.Latitude, &z.Longitude, &z.Source, &z.SourceID, &z.Properties,
		&z.CreatedAt, &z.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip zcta")
	}
	return &z, nil
}

func pipCongressionalDistrict(ctx context.Context, pool db.Pool, lng, lat float64) (*CongressionalDistrict, error) {
	sql := `
		SELECT id, geoid, state_fips, district, congress, name, lsad,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.congressional_districts
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		LIMIT 1
	`
	var cd CongressionalDistrict
	err := pool.QueryRow(ctx, sql, lng, lat).Scan(
		&cd.ID, &cd.GEOID, &cd.StateFIPS, &cd.District, &cd.Congress, &cd.Name, &cd.LSAD,
		&cd.Latitude, &cd.Longitude, &cd.Source, &cd.SourceID, &cd.Properties,
		&cd.CreatedAt, &cd.UpdatedAt,
	)
	if err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "geo: pip congressional district")
	}
	return &cd, nil
}
