-- Nearest distance from each parcel centroid to infrastructure types.
-- One row per parcel; columns for each infrastructure category.
-- Populated by the proximity_matrix analyzer.
CREATE TABLE IF NOT EXISTS geo.parcel_proximity (
    id                        SERIAL PRIMARY KEY,
    parcel_geoid              TEXT NOT NULL UNIQUE,
    parcel_geom               GEOMETRY(Point, 4326),

    -- Distances in meters to nearest feature of each type.
    dist_power_plant          DOUBLE PRECISION,
    dist_substation           DOUBLE PRECISION,
    dist_transmission_line    DOUBLE PRECISION,
    dist_pipeline             DOUBLE PRECISION,
    dist_telecom_tower        DOUBLE PRECISION,
    dist_epa_site             DOUBLE PRECISION,
    dist_flood_zone           DOUBLE PRECISION,
    dist_wetland              DOUBLE PRECISION,
    dist_primary_road         DOUBLE PRECISION,
    dist_highway              DOUBLE PRECISION,
    dist_hospital             DOUBLE PRECISION,
    dist_school               DOUBLE PRECISION,
    dist_airport              DOUBLE PRECISION,
    dist_fire_station         DOUBLE PRECISION,
    dist_water_body           DOUBLE PRECISION,

    -- Nearest feature identifiers for drill-down.
    nearest_power_plant_id    TEXT,
    nearest_substation_id     TEXT,
    nearest_epa_site_id       TEXT,
    nearest_flood_zone_id     TEXT,

    -- Census context.
    county_geoid              TEXT,
    cbsa_code                 TEXT,
    census_tract_geoid        TEXT,

    -- Metadata.
    computed_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_version            TEXT,
    properties                JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_parcel_proximity_geom
    ON geo.parcel_proximity USING GIST (parcel_geom);
CREATE INDEX IF NOT EXISTS idx_parcel_proximity_county
    ON geo.parcel_proximity (county_geoid);
CREATE INDEX IF NOT EXISTS idx_parcel_proximity_cbsa
    ON geo.parcel_proximity (cbsa_code);
