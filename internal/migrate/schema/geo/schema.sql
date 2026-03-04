-- schema.sql: Geo schema setup and migration tracking.
-- Compiled from geospatial migrations 001.

CREATE SCHEMA IF NOT EXISTS geo;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS geo.schema_migrations (
    id         SERIAL PRIMARY KEY,
    filename   TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Geocode cache for deduplicating geocoding calls.
-- Source: migration 006.
CREATE TABLE IF NOT EXISTS geo.geocode_cache (
    address_hash TEXT PRIMARY KEY,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    quality      TEXT,
    rating       INTEGER,
    matched      BOOLEAN NOT NULL DEFAULT false,
    county_fips  TEXT,
    source       TEXT NOT NULL DEFAULT 'cascade',
    cached_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_geo_geocode_cache_cached_at ON geo.geocode_cache (cached_at);

-- Geocode queue for async batch geocoding.
-- Source: migration 007.
CREATE TABLE IF NOT EXISTS geo.geocode_queue (
    id           SERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    address      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending, processing, complete, failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    result       JSONB,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_table, source_id)
);

CREATE INDEX IF NOT EXISTS idx_geocode_queue_status ON geo.geocode_queue (status);
CREATE INDEX IF NOT EXISTS idx_geocode_queue_source ON geo.geocode_queue (source_table, source_id);

-- Demographics (ACS/Census data by geography).
-- Source: migration 004.
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

CREATE INDEX IF NOT EXISTS idx_demographics_geom ON geo.demographics USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_demographics_geoid ON geo.demographics (geoid);
CREATE INDEX IF NOT EXISTS idx_demographics_geo_level ON geo.demographics (geo_level);
CREATE INDEX IF NOT EXISTS idx_demographics_year ON geo.demographics (year);
