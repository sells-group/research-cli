-- analysis.sql: Analysis run tracking, parcel proximity matrix, and composite scores.
-- Compiled from analysis migrations 100, 101, 102.

-- ============================================================================
-- Analysis run history tracking
-- Source: migration 100.
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.analysis_log (
    id            SERIAL PRIMARY KEY,
    analyzer      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running',
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    rows_affected BIGINT DEFAULT 0,
    error         TEXT,
    metadata      JSONB
);

CREATE INDEX IF NOT EXISTS idx_analysis_log_analyzer ON geo.analysis_log (analyzer);
CREATE INDEX IF NOT EXISTS idx_analysis_log_status ON geo.analysis_log (status);
CREATE INDEX IF NOT EXISTS idx_analysis_log_started_at ON geo.analysis_log (started_at DESC);

-- ============================================================================
-- Parcel proximity matrix
-- Nearest distance from each parcel centroid to infrastructure types.
-- One row per parcel; columns for each infrastructure category.
-- Populated by the proximity_matrix analyzer.
-- Source: migration 101.
-- ============================================================================
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

-- ============================================================================
-- Parcel composite scores
-- Composite scoring for each parcel based on proximity, demographics,
-- environmental factors, and infrastructure access.
-- Populated by the parcel_scores analyzer, which depends on proximity_matrix.
-- Source: migration 102.
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.parcel_scores (
    id                      SERIAL PRIMARY KEY,
    parcel_geoid            TEXT NOT NULL UNIQUE,

    -- Component scores (0.0 - 1.0).
    infrastructure_score    DOUBLE PRECISION,
    environmental_risk      DOUBLE PRECISION,
    connectivity_score      DOUBLE PRECISION,
    demographic_score       DOUBLE PRECISION,
    flood_risk              DOUBLE PRECISION,

    -- Composite scores.
    composite_score         DOUBLE PRECISION,
    opportunity_rank        INTEGER,

    -- Score metadata.
    weight_config           JSONB DEFAULT '{}',
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    properties              JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_parcel_scores_composite
    ON geo.parcel_scores (composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_parcel_scores_rank
    ON geo.parcel_scores (opportunity_rank);
CREATE INDEX IF NOT EXISTS idx_parcel_scores_geoid
    ON geo.parcel_scores (parcel_geoid);
