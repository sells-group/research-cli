-- GIST spatial indexes
CREATE INDEX IF NOT EXISTS idx_counties_geom ON geo.counties USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_places_geom ON geo.places USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_zcta_geom ON geo.zcta USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_cbsa_geom ON geo.cbsa USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_census_tracts_geom ON geo.census_tracts USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_congressional_districts_geom ON geo.congressional_districts USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_poi_geom ON geo.poi USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_infrastructure_geom ON geo.infrastructure USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_epa_sites_geom ON geo.epa_sites USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_flood_zones_geom ON geo.flood_zones USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_demographics_geom ON geo.demographics USING GIST (geom);

-- B-tree indexes
CREATE INDEX IF NOT EXISTS idx_counties_state_fips ON geo.counties (state_fips);
CREATE INDEX IF NOT EXISTS idx_places_state_fips ON geo.places (state_fips);
CREATE INDEX IF NOT EXISTS idx_census_tracts_state_fips ON geo.census_tracts (state_fips);
CREATE INDEX IF NOT EXISTS idx_congressional_districts_state_fips ON geo.congressional_districts (state_fips);
CREATE INDEX IF NOT EXISTS idx_poi_category ON geo.poi (category);
CREATE INDEX IF NOT EXISTS idx_poi_source ON geo.poi (source);
CREATE INDEX IF NOT EXISTS idx_infrastructure_type ON geo.infrastructure (type);
CREATE INDEX IF NOT EXISTS idx_infrastructure_source ON geo.infrastructure (source);
CREATE INDEX IF NOT EXISTS idx_epa_sites_program ON geo.epa_sites (program);
CREATE INDEX IF NOT EXISTS idx_flood_zones_zone_code ON geo.flood_zones (zone_code);
CREATE INDEX IF NOT EXISTS idx_demographics_geoid ON geo.demographics (geoid);
CREATE INDEX IF NOT EXISTS idx_demographics_geo_level ON geo.demographics (geo_level);
CREATE INDEX IF NOT EXISTS idx_demographics_year ON geo.demographics (year);

-- GIN full-text search on POI names
CREATE INDEX IF NOT EXISTS idx_poi_name_fts ON geo.poi USING GIN (to_tsvector('english', name));
