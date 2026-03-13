-- +goose Up
CREATE TABLE IF NOT EXISTS geo.earthquakes (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL,
    magnitude     DOUBLE PRECISION NOT NULL,
    mag_type      TEXT,
    place         TEXT,
    event_time    TIMESTAMPTZ NOT NULL,
    depth_km      DOUBLE PRECISION,
    status        TEXT,
    tsunami       BOOLEAN DEFAULT false,
    significance  INT,
    felt          INT,
    alert         TEXT,
    cdi           DOUBLE PRECISION,
    mmi           DOUBLE PRECISION,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    geom          GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                  (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    source        TEXT NOT NULL DEFAULT 'usgs',
    source_id     TEXT NOT NULL,
    properties    JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_earthquakes_event_time ON geo.earthquakes (event_time);
CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON geo.earthquakes (magnitude);
CREATE INDEX IF NOT EXISTS idx_earthquakes_geom ON geo.earthquakes USING GIST (geom);

-- +goose Down
DROP TABLE IF EXISTS geo.earthquakes;
