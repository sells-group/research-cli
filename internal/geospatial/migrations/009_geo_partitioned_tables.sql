-- Partitioned counties table by state_fips for large-state query optimization.
-- This creates a partitioned variant alongside the existing monolithic table,
-- enabling migration at the application level.

CREATE TABLE IF NOT EXISTS geo.counties_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

-- Partitioned census tracts table by state_fips.
CREATE TABLE IF NOT EXISTS geo.census_tracts_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    tract_ce    TEXT NOT NULL,
    name        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

-- Partitioned places table by state_fips.
CREATE TABLE IF NOT EXISTS geo.places_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    place_fips  TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    class_fips  TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

-- Default partitions for data that doesn't match any state.
CREATE TABLE IF NOT EXISTS geo.counties_partitioned_default
    PARTITION OF geo.counties_partitioned DEFAULT;

CREATE TABLE IF NOT EXISTS geo.census_tracts_partitioned_default
    PARTITION OF geo.census_tracts_partitioned DEFAULT;

CREATE TABLE IF NOT EXISTS geo.places_partitioned_default
    PARTITION OF geo.places_partitioned DEFAULT;
