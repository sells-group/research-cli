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
CREATE UNIQUE INDEX IF NOT EXISTS idx_wetlands_source_id ON geo.wetlands(source, source_id);
CREATE INDEX IF NOT EXISTS idx_wetlands_geom ON geo.wetlands USING GIST(geom);

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
CREATE UNIQUE INDEX IF NOT EXISTS idx_soils_source_id ON geo.soils(source, source_id);
CREATE INDEX IF NOT EXISTS idx_soils_geom ON geo.soils USING GIST(geom);
