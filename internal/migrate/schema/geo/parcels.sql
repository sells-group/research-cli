-- parcels.sql: Land parcel boundaries.
-- Compiled from geospatial migration 017.

CREATE TABLE IF NOT EXISTS geo.parcels (
    id           SERIAL PRIMARY KEY,
    parcel_geoid TEXT NOT NULL UNIQUE,
    geom         GEOMETRY(MultiPolygon, 4326),
    centroid     GEOMETRY(Point, 4326),
    county_geoid TEXT,
    state_fips   TEXT,
    acreage      DOUBLE PRECISION,
    source       TEXT NOT NULL,
    source_id    TEXT,
    properties   JSONB DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parcels_centroid ON geo.parcels USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_parcels_geom ON geo.parcels USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcels_county ON geo.parcels (county_geoid);
CREATE INDEX IF NOT EXISTS idx_parcels_state ON geo.parcels (state_fips);
