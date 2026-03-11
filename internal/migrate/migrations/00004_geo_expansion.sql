-- +goose Up

-- Block groups (per-state TIGER/Line download).
CREATE TABLE IF NOT EXISTS geo.block_groups (
    id            BIGSERIAL PRIMARY KEY,
    geoid         TEXT NOT NULL UNIQUE,
    state_fips    TEXT,
    county_fips   TEXT,
    tract_ce      TEXT,
    blkgrp_ce     TEXT,
    geom          geometry(MultiPolygon, 4326),
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    source        TEXT NOT NULL DEFAULT 'tiger',
    source_id     TEXT NOT NULL,
    properties    JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_block_groups_geoid ON geo.block_groups (geoid);
CREATE INDEX IF NOT EXISTS idx_block_groups_state ON geo.block_groups (state_fips);
CREATE INDEX IF NOT EXISTS idx_block_groups_geom ON geo.block_groups USING GIST (geom);

-- County subdivisions (per-state TIGER/Line download).
CREATE TABLE IF NOT EXISTS geo.county_subdivisions (
    id            BIGSERIAL PRIMARY KEY,
    geoid         TEXT NOT NULL UNIQUE,
    state_fips    TEXT,
    county_fips   TEXT,
    cousub_fips   TEXT,
    name          TEXT,
    lsad          TEXT,
    class_fips    TEXT,
    geom          geometry(MultiPolygon, 4326),
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    source        TEXT NOT NULL DEFAULT 'tiger',
    source_id     TEXT NOT NULL,
    properties    JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_county_subdivisions_geoid ON geo.county_subdivisions (geoid);
CREATE INDEX IF NOT EXISTS idx_county_subdivisions_state ON geo.county_subdivisions (state_fips);
CREATE INDEX IF NOT EXISTS idx_county_subdivisions_geom ON geo.county_subdivisions USING GIST (geom);

-- Water features (per-county TIGER/Line area + linear water).
CREATE TABLE IF NOT EXISTS geo.water_features (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT,
    water_type    TEXT,
    mtfcc         TEXT,
    geom          geometry(Geometry, 4326),
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    source        TEXT NOT NULL DEFAULT 'tiger',
    source_id     TEXT NOT NULL,
    properties    JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_water_features_type ON geo.water_features (water_type);
CREATE INDEX IF NOT EXISTS idx_water_features_geom ON geo.water_features USING GIST (geom);

-- +goose Down
DROP TABLE IF EXISTS geo.water_features;
DROP TABLE IF EXISTS geo.county_subdivisions;
DROP TABLE IF EXISTS geo.block_groups;
