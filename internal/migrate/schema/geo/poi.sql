-- poi.sql: Points of interest, infrastructure, and EPA site tables.
-- Compiled from geospatial migrations 003, 005, 010, 012, 016.

-- ============================================================================
-- Points of interest
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.poi (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    subcategory TEXT,
    address     TEXT,
    geom        GEOMETRY(Point, 4326),
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    source      TEXT NOT NULL,
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_poi_geom ON geo.poi USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_poi_category ON geo.poi (category);
CREATE INDEX IF NOT EXISTS idx_poi_source ON geo.poi (source);
CREATE INDEX IF NOT EXISTS idx_poi_name_fts ON geo.poi USING GIN (to_tsvector('english', name));
-- Unique constraint for upserts (migration 016).
CREATE UNIQUE INDEX IF NOT EXISTS idx_poi_source_source_id ON geo.poi (source, source_id);

-- ============================================================================
-- Infrastructure assets
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.infrastructure (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL, -- power_plant, substation, telecom_tower, pipeline
    fuel_type   TEXT,
    capacity    DOUBLE PRECISION,
    geom        GEOMETRY(Point, 4326),
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    source      TEXT NOT NULL,
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_infrastructure_geom ON geo.infrastructure USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_infrastructure_type ON geo.infrastructure (type);
CREATE INDEX IF NOT EXISTS idx_infrastructure_source ON geo.infrastructure (source);
-- Unique constraint for upserts (migration 010).
CREATE UNIQUE INDEX IF NOT EXISTS idx_infrastructure_source_source_id ON geo.infrastructure (source, source_id);

-- ============================================================================
-- EPA monitored sites
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.epa_sites (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    program     TEXT NOT NULL,
    registry_id TEXT UNIQUE,
    status      TEXT,
    geom        GEOMETRY(Point, 4326),
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    source      TEXT NOT NULL DEFAULT 'epa',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_epa_sites_geom ON geo.epa_sites USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_epa_sites_program ON geo.epa_sites (program);
-- Unique constraint for upserts (migration 012).
CREATE UNIQUE INDEX IF NOT EXISTS idx_epa_sites_source_source_id ON geo.epa_sites (source, source_id);
