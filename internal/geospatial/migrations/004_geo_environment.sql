-- Flood zones
CREATE TABLE IF NOT EXISTS geo.flood_zones (
    id          SERIAL PRIMARY KEY,
    zone_code   TEXT NOT NULL,
    flood_type  TEXT NOT NULL,
    geom        GEOMETRY(MultiPolygon, 4326),
    source      TEXT NOT NULL DEFAULT 'fema',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Demographics (ACS/Census data by geography)
CREATE TABLE IF NOT EXISTS geo.demographics (
    id               SERIAL PRIMARY KEY,
    geoid            TEXT NOT NULL,
    geo_level        TEXT NOT NULL, -- county, place, zcta, tract
    year             INTEGER NOT NULL,
    total_population INTEGER,
    median_income    DOUBLE PRECISION,
    median_age       DOUBLE PRECISION,
    housing_units    INTEGER,
    geom             GEOMETRY(MultiPolygon, 4326),
    source           TEXT NOT NULL DEFAULT 'census',
    source_id        TEXT,
    properties       JSONB DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (geoid, geo_level, year)
);
