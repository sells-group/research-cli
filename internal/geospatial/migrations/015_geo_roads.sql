CREATE TABLE IF NOT EXISTS geo.roads (
    id         SERIAL PRIMARY KEY,
    name       TEXT,
    route_type TEXT NOT NULL,
    mtfcc      TEXT,
    geom       GEOMETRY(MultiLineString, 4326),
    source     TEXT NOT NULL DEFAULT 'tiger',
    source_id  TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_roads_source_id ON geo.roads(source, source_id);
CREATE INDEX IF NOT EXISTS idx_roads_geom ON geo.roads USING GIST(geom);
