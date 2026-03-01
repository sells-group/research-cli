-- Points of interest
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

-- Infrastructure assets
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

-- EPA monitored sites
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
