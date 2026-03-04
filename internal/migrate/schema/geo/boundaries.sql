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

-- ADV firms by state (aggregated from fed_data.adv_firms).
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_adv_firms_by_state AS
SELECT
    af.state,
    COUNT(af.crd_number)        AS firm_count,
    SUM(af.aum)                 AS total_aum,
    SUM(af.num_employees)       AS total_employees,
    SUM(af.num_accounts)        AS total_accounts
FROM fed_data.adv_firms af
WHERE af.state IS NOT NULL
GROUP BY af.state;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_adv_firms_by_state_pk
    ON geo.mv_adv_firms_by_state (state);
