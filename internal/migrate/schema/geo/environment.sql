-- environment.sql: Flood zones, broadband coverage, and natural resources.
-- Compiled from geospatial migrations 004, 005, 011, 013, 014.

-- ============================================================================
-- Flood zones
-- ============================================================================
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

CREATE INDEX IF NOT EXISTS idx_flood_zones_geom ON geo.flood_zones USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_flood_zones_zone_code ON geo.flood_zones (zone_code);
-- Unique constraint for upserts (migration 011).
CREATE UNIQUE INDEX IF NOT EXISTS idx_flood_zones_source_source_id ON geo.flood_zones (source, source_id);

-- ============================================================================
-- Broadband coverage (FCC data)
-- Source: migration 013.
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.broadband_coverage (
    id             SERIAL PRIMARY KEY,
    block_geoid    TEXT NOT NULL,
    technology     TEXT NOT NULL,
    max_download   DOUBLE PRECISION,
    max_upload     DOUBLE PRECISION,
    provider_count INTEGER,
    geom           GEOMETRY(Point, 4326),
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    source         TEXT NOT NULL DEFAULT 'fcc',
    source_id      TEXT,
    properties     JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_broadband_source_id ON geo.broadband_coverage (source, source_id);
CREATE INDEX IF NOT EXISTS idx_broadband_geom ON geo.broadband_coverage USING GIST (geom);

-- ============================================================================
-- Wetlands (NWI)
-- Source: migration 014.
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.wetlands (
    id           SERIAL PRIMARY KEY,
    wetland_type TEXT NOT NULL,
    attribute    TEXT,
    acres        DOUBLE PRECISION,
    geom         GEOMETRY(MultiPolygon, 4326),
    source       TEXT NOT NULL DEFAULT 'nwi',
    source_id    TEXT,
    properties   JSONB DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wetlands_source_id ON geo.wetlands (source, source_id);
CREATE INDEX IF NOT EXISTS idx_wetlands_geom ON geo.wetlands USING GIST (geom);

-- ============================================================================
-- Soils (NRCS)
-- Source: migration 014.
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.soils (
    id             SERIAL PRIMARY KEY,
    mukey          TEXT NOT NULL,
    muname         TEXT,
    drainage_class TEXT,
    hydric_rating  TEXT,
    geom           GEOMETRY(MultiPolygon, 4326),
    source         TEXT NOT NULL DEFAULT 'nrcs',
    source_id      TEXT,
    properties     JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_soils_source_id ON geo.soils (source, source_id);
CREATE INDEX IF NOT EXISTS idx_soils_geom ON geo.soils USING GIST (geom);
