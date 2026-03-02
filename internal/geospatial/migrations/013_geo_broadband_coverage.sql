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
CREATE UNIQUE INDEX IF NOT EXISTS idx_broadband_source_id ON geo.broadband_coverage(source, source_id);
CREATE INDEX IF NOT EXISTS idx_broadband_geom ON geo.broadband_coverage USING GIST(geom);
