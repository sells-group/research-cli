package geospatial

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// PostgresStore implements Store using a Postgres connection pool with PostGIS.
type PostgresStore struct {
	pool db.Pool
}

// NewPostgresStore creates a new PostgresStore.
func NewPostgresStore(pool db.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// UpsertCounty implements Store.
func (s *PostgresStore) UpsertCounty(ctx context.Context, c *County) error {
	sql := `
		INSERT INTO geo.counties (geoid, state_fips, county_fips, name, lsad, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (geoid) DO UPDATE SET
			state_fips = EXCLUDED.state_fips,
			county_fips = EXCLUDED.county_fips,
			name = EXCLUDED.name,
			lsad = EXCLUDED.lsad,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(c.Properties)
	_, err := s.pool.Exec(ctx, sql,
		c.GEOID, c.StateFIPS, c.CountyFIPS, c.Name, c.LSAD,
		c.Latitude, c.Longitude, c.Source, c.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert county")
}

// GetCounty implements Store.
func (s *PostgresStore) GetCounty(ctx context.Context, geoid string) (*County, error) {
	sql := `
		SELECT id, geoid, state_fips, county_fips, name, lsad,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.counties WHERE geoid = $1
	`
	var c County
	err := s.pool.QueryRow(ctx, sql, geoid).Scan(
		&c.ID, &c.GEOID, &c.StateFIPS, &c.CountyFIPS, &c.Name, &c.LSAD,
		&c.Latitude, &c.Longitude, &c.Source, &c.SourceID, &c.Properties,
		&c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		return nil, eris.Wrap(err, "geo: get county")
	}
	return &c, nil
}

// ListCountiesByState implements Store.
func (s *PostgresStore) ListCountiesByState(ctx context.Context, stateFIPS string) ([]County, error) {
	sql := `
		SELECT id, geoid, state_fips, county_fips, name, lsad,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.counties WHERE state_fips = $1 ORDER BY name
	`
	rows, err := s.pool.Query(ctx, sql, stateFIPS)
	if err != nil {
		return nil, eris.Wrap(err, "geo: list counties by state")
	}
	defer rows.Close()

	var counties []County
	for rows.Next() {
		var c County
		if err := rows.Scan(
			&c.ID, &c.GEOID, &c.StateFIPS, &c.CountyFIPS, &c.Name, &c.LSAD,
			&c.Latitude, &c.Longitude, &c.Source, &c.SourceID, &c.Properties,
			&c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, eris.Wrap(err, "geo: scan county row")
		}
		counties = append(counties, c)
	}
	return counties, rows.Err()
}

// BulkUpsertCounties implements Store.
func (s *PostgresStore) BulkUpsertCounties(ctx context.Context, counties []County) (int64, error) {
	rows := make([][]any, len(counties))
	for i, c := range counties {
		rows[i] = []any{
			c.GEOID, c.StateFIPS, c.CountyFIPS, c.Name, c.LSAD,
			c.Latitude, c.Longitude, c.Source, c.SourceID, normalizeProperties(c.Properties),
		}
	}
	return db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "geo.counties",
		Columns:      []string{"geoid", "state_fips", "county_fips", "name", "lsad", "latitude", "longitude", "source", "source_id", "properties"},
		ConflictKeys: []string{"geoid"},
	}, rows)
}

// UpsertPlace implements Store.
func (s *PostgresStore) UpsertPlace(ctx context.Context, p *Place) error {
	sql := `
		INSERT INTO geo.places (geoid, state_fips, place_fips, name, lsad, class_fips, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (geoid) DO UPDATE SET
			state_fips = EXCLUDED.state_fips,
			place_fips = EXCLUDED.place_fips,
			name = EXCLUDED.name,
			lsad = EXCLUDED.lsad,
			class_fips = EXCLUDED.class_fips,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(p.Properties)
	_, err := s.pool.Exec(ctx, sql,
		p.GEOID, p.StateFIPS, p.PlaceFIPS, p.Name, p.LSAD, p.ClassFIPS,
		p.Latitude, p.Longitude, p.Source, p.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert place")
}

// UpsertCBSA implements Store.
func (s *PostgresStore) UpsertCBSA(ctx context.Context, c *CBSA) error {
	sql := `
		INSERT INTO geo.cbsa (cbsa_code, name, lsad, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (cbsa_code) DO UPDATE SET
			name = EXCLUDED.name,
			lsad = EXCLUDED.lsad,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(c.Properties)
	_, err := s.pool.Exec(ctx, sql,
		c.CBSACode, c.Name, c.LSAD,
		c.Latitude, c.Longitude, c.Source, c.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert cbsa")
}

// GetCBSA implements Store.
func (s *PostgresStore) GetCBSA(ctx context.Context, cbsaCode string) (*CBSA, error) {
	sql := `
		SELECT id, cbsa_code, name, lsad, latitude, longitude,
		       source, source_id, properties, created_at, updated_at
		FROM geo.cbsa WHERE cbsa_code = $1
	`
	var c CBSA
	err := s.pool.QueryRow(ctx, sql, cbsaCode).Scan(
		&c.ID, &c.CBSACode, &c.Name, &c.LSAD,
		&c.Latitude, &c.Longitude, &c.Source, &c.SourceID, &c.Properties,
		&c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		return nil, eris.Wrap(err, "geo: get cbsa")
	}
	return &c, nil
}

// UpsertPOI implements Store.
func (s *PostgresStore) UpsertPOI(ctx context.Context, p *POI) error {
	sql := `
		INSERT INTO geo.poi (name, category, subcategory, address, geom, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($6, $5), 4326), $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			category = EXCLUDED.category,
			subcategory = EXCLUDED.subcategory,
			address = EXCLUDED.address,
			geom = EXCLUDED.geom,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(p.Properties)
	_, err := s.pool.Exec(ctx, sql,
		p.Name, p.Category, p.Subcategory, p.Address,
		p.Latitude, p.Longitude, p.Source, p.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert poi")
}

// GetPOI implements Store.
func (s *PostgresStore) GetPOI(ctx context.Context, id int) (*POI, error) {
	sql := `
		SELECT id, name, category, subcategory, address,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.poi WHERE id = $1
	`
	var p POI
	err := s.pool.QueryRow(ctx, sql, id).Scan(
		&p.ID, &p.Name, &p.Category, &p.Subcategory, &p.Address,
		&p.Latitude, &p.Longitude, &p.Source, &p.SourceID, &p.Properties,
		&p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, eris.Wrap(err, "geo: get poi")
	}
	return &p, nil
}

// ListPOIByCategory implements Store.
func (s *PostgresStore) ListPOIByCategory(ctx context.Context, category string, limit, offset int) ([]POI, int, error) {
	countSQL := `SELECT COUNT(*) FROM geo.poi WHERE category = $1`
	var total int
	if err := s.pool.QueryRow(ctx, countSQL, category).Scan(&total); err != nil {
		return nil, 0, eris.Wrap(err, "geo: count poi by category")
	}

	sql := `
		SELECT id, name, category, subcategory, address,
		       latitude, longitude, source, source_id, properties,
		       created_at, updated_at
		FROM geo.poi WHERE category = $1 ORDER BY name LIMIT $2 OFFSET $3
	`
	rows, err := s.pool.Query(ctx, sql, category, limit, offset)
	if err != nil {
		return nil, 0, eris.Wrap(err, "geo: list poi by category")
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
			return nil, 0, eris.Wrap(err, "geo: scan poi row")
		}
		pois = append(pois, p)
	}
	return pois, total, rows.Err()
}

// BulkUpsertPOI implements Store.
func (s *PostgresStore) BulkUpsertPOI(ctx context.Context, pois []POI) (int64, error) {
	rows := make([][]any, len(pois))
	for i, p := range pois {
		rows[i] = []any{
			p.Name, p.Category, p.Subcategory, p.Address,
			p.Latitude, p.Longitude, p.Source, p.SourceID, normalizeProperties(p.Properties),
		}
	}
	return db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "geo.poi",
		Columns:      []string{"name", "category", "subcategory", "address", "latitude", "longitude", "source", "source_id", "properties"},
		ConflictKeys: []string{"id"},
	}, rows)
}

// UpsertInfrastructure implements Store.
func (s *PostgresStore) UpsertInfrastructure(ctx context.Context, infra *Infrastructure) error {
	sql := `
		INSERT INTO geo.infrastructure (name, type, fuel_type, capacity, geom, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($6, $5), 4326), $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			type = EXCLUDED.type,
			fuel_type = EXCLUDED.fuel_type,
			capacity = EXCLUDED.capacity,
			geom = EXCLUDED.geom,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(infra.Properties)
	_, err := s.pool.Exec(ctx, sql,
		infra.Name, infra.Type, infra.FuelType, infra.Capacity,
		infra.Latitude, infra.Longitude, infra.Source, infra.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert infrastructure")
}

// BulkUpsertInfrastructure implements Store.
func (s *PostgresStore) BulkUpsertInfrastructure(ctx context.Context, infras []Infrastructure) (int64, error) {
	rows := make([][]any, len(infras))
	for i, infra := range infras {
		rows[i] = []any{
			infra.Name, infra.Type, infra.FuelType, infra.Capacity,
			infra.Latitude, infra.Longitude, infra.Source, infra.SourceID, normalizeProperties(infra.Properties),
		}
	}
	return db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "geo.infrastructure",
		Columns:      []string{"name", "type", "fuel_type", "capacity", "latitude", "longitude", "source", "source_id", "properties"},
		ConflictKeys: []string{"id"},
	}, rows)
}

// UpsertEPASite implements Store.
func (s *PostgresStore) UpsertEPASite(ctx context.Context, site *EPASite) error {
	sql := `
		INSERT INTO geo.epa_sites (name, program, registry_id, status, geom, latitude, longitude, source, source_id, properties)
		VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($7, $6), 4326), $6, $7, $8, $9, $10)
		ON CONFLICT (registry_id) DO UPDATE SET
			name = EXCLUDED.name,
			program = EXCLUDED.program,
			status = EXCLUDED.status,
			geom = EXCLUDED.geom,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(site.Properties)
	_, err := s.pool.Exec(ctx, sql,
		site.Name, site.Program, site.RegistryID, site.Status,
		site.Latitude, site.Longitude, site.Source, site.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert epa site")
}

// UpsertFloodZone implements Store.
func (s *PostgresStore) UpsertFloodZone(ctx context.Context, fz *FloodZone) error {
	sql := `
		INSERT INTO geo.flood_zones (zone_code, flood_type, source, source_id, properties)
		VALUES ($1, $2, $3, $4, $5)
	`
	props := normalizeProperties(fz.Properties)
	_, err := s.pool.Exec(ctx, sql,
		fz.ZoneCode, fz.FloodType, fz.Source, fz.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert flood zone")
}

// UpsertDemographic implements Store.
func (s *PostgresStore) UpsertDemographic(ctx context.Context, d *Demographic) error {
	sql := `
		INSERT INTO geo.demographics (geoid, geo_level, year, total_population, median_income, median_age, housing_units, source, source_id, properties)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (geoid, geo_level, year) DO UPDATE SET
			total_population = EXCLUDED.total_population,
			median_income = EXCLUDED.median_income,
			median_age = EXCLUDED.median_age,
			housing_units = EXCLUDED.housing_units,
			source = EXCLUDED.source,
			source_id = EXCLUDED.source_id,
			properties = EXCLUDED.properties,
			updated_at = now()
	`
	props := normalizeProperties(d.Properties)
	_, err := s.pool.Exec(ctx, sql,
		d.GEOID, d.GeoLevel, d.Year,
		d.TotalPopulation, d.MedianIncome, d.MedianAge, d.HousingUnits,
		d.Source, d.SourceID, props,
	)
	return eris.Wrap(err, "geo: upsert demographic")
}

// GetDemographic implements Store.
func (s *PostgresStore) GetDemographic(ctx context.Context, geoid, geoLevel string, year int) (*Demographic, error) {
	sql := `
		SELECT id, geoid, geo_level, year, total_population, median_income,
		       median_age, housing_units, source, source_id, properties,
		       created_at, updated_at
		FROM geo.demographics WHERE geoid = $1 AND geo_level = $2 AND year = $3
	`
	var d Demographic
	err := s.pool.QueryRow(ctx, sql, geoid, geoLevel, year).Scan(
		&d.ID, &d.GEOID, &d.GeoLevel, &d.Year,
		&d.TotalPopulation, &d.MedianIncome, &d.MedianAge, &d.HousingUnits,
		&d.Source, &d.SourceID, &d.Properties,
		&d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, eris.Wrap(err, "geo: demographic not found")
		}
		return nil, eris.Wrap(err, "geo: get demographic")
	}
	return &d, nil
}

// normalizeProperties returns "{}" if the raw JSON is nil or empty.
func normalizeProperties(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
