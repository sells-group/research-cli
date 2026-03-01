-- County boundaries
CREATE TABLE IF NOT EXISTS geo.counties (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
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
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Place (city/town/CDP) boundaries
CREATE TABLE IF NOT EXISTS geo.places (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
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
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ZIP Code Tabulation Areas
CREATE TABLE IF NOT EXISTS geo.zcta (
    id          SERIAL PRIMARY KEY,
    zcta5       TEXT NOT NULL UNIQUE,
    state_fips  TEXT,
    aland       BIGINT,
    awater      BIGINT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Core-Based Statistical Areas (MSAs)
CREATE TABLE IF NOT EXISTS geo.cbsa (
    id          SERIAL PRIMARY KEY,
    cbsa_code   TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Census tracts
CREATE TABLE IF NOT EXISTS geo.census_tracts (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
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
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Congressional districts
CREATE TABLE IF NOT EXISTS geo.congressional_districts (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
    state_fips  TEXT NOT NULL,
    district    TEXT NOT NULL,
    congress    TEXT,
    name        TEXT,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
