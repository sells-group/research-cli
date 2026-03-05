-- +goose Up
-- Geo schema: boundaries, roads, POI, environment, parcels, analysis.

-- schema.sql: Geo schema setup and migration tracking.
-- Compiled from geospatial migrations 001.

CREATE SCHEMA IF NOT EXISTS geo;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS geo.schema_migrations (
    id         SERIAL PRIMARY KEY,
    filename   TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Geocode cache for deduplicating geocoding calls.
-- Source: migration 006.
CREATE TABLE IF NOT EXISTS geo.geocode_cache (
    address_hash TEXT PRIMARY KEY,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    quality      TEXT,
    rating       INTEGER,
    matched      BOOLEAN NOT NULL DEFAULT false,
    county_fips  TEXT,
    source       TEXT NOT NULL DEFAULT 'cascade',
    cached_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_geo_geocode_cache_cached_at ON geo.geocode_cache (cached_at);

-- Geocode queue for async batch geocoding.
-- Source: migration 007.
CREATE TABLE IF NOT EXISTS geo.geocode_queue (
    id           SERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    address      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending, processing, complete, failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    result       JSONB,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_table, source_id)
);

CREATE INDEX IF NOT EXISTS idx_geocode_queue_status ON geo.geocode_queue (status);
CREATE INDEX IF NOT EXISTS idx_geocode_queue_source ON geo.geocode_queue (source_table, source_id);

-- Demographics (ACS/Census data by geography).
-- Source: migration 004.
CREATE TABLE IF NOT EXISTS geo.demographics (
    id               SERIAL PRIMARY KEY,
    geoid            TEXT NOT NULL,
    geo_level        TEXT NOT NULL, -- county, place, zcta, tract
    year             INTEGER NOT NULL,
    total_population INTEGER,
    median_income    DOUBLE PRECISION,
    median_age       DOUBLE PRECISION,
    housing_units    INTEGER,
    geom             GEOMETRY(MultiPolygon, 4326),
    source           TEXT NOT NULL DEFAULT 'census',
    source_id        TEXT,
    properties       JSONB DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (geoid, geo_level, year)
);

CREATE INDEX IF NOT EXISTS idx_demographics_geom ON geo.demographics USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_demographics_geoid ON geo.demographics (geoid);
CREATE INDEX IF NOT EXISTS idx_demographics_geo_level ON geo.demographics (geo_level);
CREATE INDEX IF NOT EXISTS idx_demographics_year ON geo.demographics (year);

-- boundaries.sql: Administrative and statistical boundary tables.
-- Compiled from geospatial migrations 002, 005, 008 (materialized views), 009 (partitioned).

-- ============================================================================
-- County boundaries
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.counties (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_counties_geom ON geo.counties USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_counties_state_fips ON geo.counties (state_fips);

-- ============================================================================
-- Place (city/town/CDP) boundaries
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.places (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
    state_fips  TEXT NOT NULL,
    place_fips  TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    class_fips  TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_places_geom ON geo.places USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_places_state_fips ON geo.places (state_fips);

-- ============================================================================
-- ZIP Code Tabulation Areas
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.zcta (
    id          SERIAL PRIMARY KEY,
    zcta5       TEXT NOT NULL UNIQUE,
    state_fips  TEXT,
    aland       BIGINT,
    awater      BIGINT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_zcta_geom ON geo.zcta USING GIST (geom);

-- ============================================================================
-- Core-Based Statistical Areas (MSAs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.cbsa (
    id          SERIAL PRIMARY KEY,
    cbsa_code   TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cbsa_geom ON geo.cbsa USING GIST (geom);

-- ============================================================================
-- Census tracts
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.census_tracts (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    tract_ce    TEXT NOT NULL,
    name        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_census_tracts_geom ON geo.census_tracts USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_census_tracts_state_fips ON geo.census_tracts (state_fips);

-- ============================================================================
-- Congressional districts
-- ============================================================================
CREATE TABLE IF NOT EXISTS geo.congressional_districts (
    id          SERIAL PRIMARY KEY,
    geoid       TEXT NOT NULL UNIQUE,
    state_fips  TEXT NOT NULL,
    district    TEXT NOT NULL,
    congress    TEXT,
    name        TEXT,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_congressional_districts_geom ON geo.congressional_districts USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_congressional_districts_state_fips ON geo.congressional_districts (state_fips);

-- ============================================================================
-- Partitioned tables (state_fips list partitioning)
-- Source: migration 009.
-- ============================================================================

-- Partitioned counties by state_fips for large-state query optimization.
CREATE TABLE IF NOT EXISTS geo.counties_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

CREATE TABLE IF NOT EXISTS geo.counties_partitioned_default
    PARTITION OF geo.counties_partitioned DEFAULT;

-- Partitioned census tracts by state_fips.
CREATE TABLE IF NOT EXISTS geo.census_tracts_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    tract_ce    TEXT NOT NULL,
    name        TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

CREATE TABLE IF NOT EXISTS geo.census_tracts_partitioned_default
    PARTITION OF geo.census_tracts_partitioned DEFAULT;

-- Partitioned places by state_fips.
CREATE TABLE IF NOT EXISTS geo.places_partitioned (
    id          SERIAL,
    geoid       TEXT NOT NULL,
    state_fips  TEXT NOT NULL,
    place_fips  TEXT NOT NULL,
    name        TEXT NOT NULL,
    lsad        TEXT,
    class_fips  TEXT,
    geom        GEOMETRY(MultiPolygon, 4326),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    source      TEXT NOT NULL DEFAULT 'tiger',
    source_id   TEXT,
    properties  JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_fips, id),
    UNIQUE (state_fips, geoid)
) PARTITION BY LIST (state_fips);

CREATE TABLE IF NOT EXISTS geo.places_partitioned_default
    PARTITION OF geo.places_partitioned DEFAULT;

-- roads.sql: Road network from TIGER/Line.
-- Compiled from geospatial migration 015.

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

CREATE UNIQUE INDEX IF NOT EXISTS idx_roads_source_id ON geo.roads (source, source_id);
CREATE INDEX IF NOT EXISTS idx_roads_geom ON geo.roads USING GIST (geom);

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

-- parcels.sql: Land parcel boundaries.
-- Compiled from geospatial migration 017.

CREATE TABLE IF NOT EXISTS geo.parcels (
    id           SERIAL PRIMARY KEY,
    parcel_geoid TEXT NOT NULL UNIQUE,
    geom         GEOMETRY(MultiPolygon, 4326),
    centroid     GEOMETRY(Point, 4326),
    county_geoid TEXT,
    state_fips   TEXT,
    acreage      DOUBLE PRECISION,
    source       TEXT NOT NULL,
    source_id    TEXT,
    properties   JSONB DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parcels_centroid ON geo.parcels USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_parcels_geom ON geo.parcels USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcels_county ON geo.parcels (county_geoid);
CREATE INDEX IF NOT EXISTS idx_parcels_state ON geo.parcels (state_fips);

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

-- ============================================================================
-- Materialized views (cross-reference with fed_data)
-- Source: migration 008.
-- ============================================================================

-- County-level economic indicators (CBP + QCEW joined to geo.counties).
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_county_economics AS
SELECT
    c.geoid,
    c.name                  AS county_name,
    c.state_fips,
    cbp.year,
    cbp.est                 AS cbp_establishments,
    cbp.emp                 AS cbp_employment,
    cbp.ap                  AS cbp_annual_payroll,
    qcew.total_qtrly_wages  AS qcew_quarterly_wages,
    qcew.month1_emplvl      AS qcew_employment
FROM geo.counties c
LEFT JOIN fed_data.cbp_data cbp
    ON c.state_fips = cbp.fips_state
   AND c.county_fips = cbp.fips_county
   AND cbp.naics = '------'
LEFT JOIN LATERAL (
    SELECT q.total_qtrly_wages, q.month1_emplvl
    FROM fed_data.qcew_data q
    WHERE q.area_fips = c.state_fips || c.county_fips
      AND q.own_code = '5'
      AND q.industry_code = '10'
      AND q.year = cbp.year
    ORDER BY q.qtr DESC
    LIMIT 1
) qcew ON true
WHERE cbp.year IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_county_economics_pk
    ON geo.mv_county_economics (geoid, year);

-- CBSA-level demographics summary.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_cbsa_summary AS
SELECT
    cb.cbsa_code,
    cb.name     AS cbsa_name,
    cb.lsad,
    d.total_population,
    d.median_income,
    d.median_age,
    d.housing_units,
    d.year      AS demo_year
FROM geo.cbsa cb
LEFT JOIN geo.demographics d
    ON cb.cbsa_code = d.geoid
   AND d.geo_level = 'cbsa';

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cbsa_summary_pk
    ON geo.mv_cbsa_summary (cbsa_code, demo_year);

-- EPA facility locations matched to counties via spatial containment.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_epa_by_county AS
SELECT
    c.geoid             AS county_geoid,
    c.name              AS county_name,
    c.state_fips,
    COUNT(e.id)         AS facility_count,
    ARRAY_AGG(DISTINCT e.program) AS programs
FROM geo.counties c
JOIN geo.epa_sites e ON ST_Contains(c.geom, e.geom)
GROUP BY c.geoid, c.name, c.state_fips;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_epa_by_county_pk
    ON geo.mv_epa_by_county (county_geoid);
CREATE INDEX IF NOT EXISTS idx_mv_epa_by_county_state
    ON geo.mv_epa_by_county (state_fips);

-- Infrastructure density by county and type via spatial containment.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_infrastructure_by_county AS
SELECT
    c.geoid             AS county_geoid,
    c.name              AS county_name,
    c.state_fips,
    i.type              AS infrastructure_type,
    COUNT(i.id)         AS count,
    SUM(i.capacity)     AS total_capacity
FROM geo.counties c
JOIN geo.infrastructure i ON ST_Contains(c.geom, i.geom)
GROUP BY c.geoid, c.name, c.state_fips, i.type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_infrastructure_by_county_pk
    ON geo.mv_infrastructure_by_county (county_geoid, infrastructure_type);
CREATE INDEX IF NOT EXISTS idx_mv_infrastructure_by_county_type
    ON geo.mv_infrastructure_by_county (infrastructure_type);

-- ADV firms by state (aggregated from fed_data.adv_firms + latest filing).
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_adv_firms_by_state AS
SELECT
    af.state,
    COUNT(af.crd_number)        AS firm_count,
    SUM(lf.aum_total)           AS total_aum,
    SUM(lf.num_employees)       AS total_employees,
    SUM(lf.num_accounts)        AS total_accounts
FROM fed_data.adv_firms af
LEFT JOIN LATERAL (
    SELECT fil.aum_total, fil.num_employees, fil.num_accounts
    FROM fed_data.adv_filings fil
    WHERE fil.crd_number = af.crd_number
    ORDER BY fil.filing_date DESC
    LIMIT 1
) lf ON true
WHERE af.state IS NOT NULL
GROUP BY af.state;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_adv_firms_by_state_pk
    ON geo.mv_adv_firms_by_state (state);

-- +goose Down
-- Initial schema migration: no rollback.
