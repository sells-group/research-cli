-- 083_tiger_geocoder.sql: PostGIS tiger geocoder extension + load tracking.
-- Creates tiger_data schema via extension, adds load_status for tracking TIGER/Line data loads.

-- Enable PostGIS tiger geocoder (creates tiger_data schema, geocode() function,
-- normalize_address(), pprint_addy(), reverse_geocode(), and all required lookup tables).
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder CASCADE;

-- Track which TIGER/Line data has been loaded, by state + table + year.
CREATE TABLE IF NOT EXISTS tiger_data.load_status (
    id          SERIAL PRIMARY KEY,
    state_fips  VARCHAR(2) NOT NULL,
    state_abbr  VARCHAR(2) NOT NULL,
    table_name  VARCHAR(50) NOT NULL,
    year        INTEGER NOT NULL,
    row_count   INTEGER NOT NULL DEFAULT 0,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INTEGER,
    UNIQUE (state_fips, table_name, year)
);

COMMENT ON TABLE tiger_data.load_status IS 'Tracks TIGER/Line shapefile data loads by state, table, and year';
