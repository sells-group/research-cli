-- Composite scoring for each parcel based on proximity, demographics,
-- environmental factors, and infrastructure access.
-- Populated by the parcel_scores analyzer, which depends on proximity_matrix.
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
