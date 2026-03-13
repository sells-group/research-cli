-- +goose Up

-- HUD LIHTC projects (Low-Income Housing Tax Credit).
CREATE TABLE IF NOT EXISTS geo.lihtc_projects (
    id              BIGSERIAL PRIMARY KEY,
    project_id      TEXT NOT NULL,
    project_name    TEXT,
    project_state   TEXT,
    project_zip     TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    total_units     INT,
    li_units        INT,
    year_placed     INT,
    source          TEXT NOT NULL DEFAULT 'hud_lihtc',
    source_id       TEXT NOT NULL,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_lihtc_state ON geo.lihtc_projects (project_state);
CREATE INDEX IF NOT EXISTS idx_lihtc_zip ON geo.lihtc_projects (project_zip);
CREATE INDEX IF NOT EXISTS idx_lihtc_year ON geo.lihtc_projects (year_placed);

-- HUD Fair Market Rents by county.
CREATE TABLE IF NOT EXISTS geo.fair_market_rents (
    id              BIGSERIAL PRIMARY KEY,
    fips            TEXT NOT NULL,
    state_fips      TEXT,
    county_name     TEXT,
    year            INT NOT NULL,
    fmr_0br         INT,
    fmr_1br         INT,
    fmr_2br         INT,
    fmr_3br         INT,
    fmr_4br         INT,
    source          TEXT NOT NULL DEFAULT 'hud_fmr',
    source_id       TEXT NOT NULL,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (fips, year)
);
CREATE INDEX IF NOT EXISTS idx_fmr_fips ON geo.fair_market_rents (fips);
CREATE INDEX IF NOT EXISTS idx_fmr_state ON geo.fair_market_rents (state_fips);
CREATE INDEX IF NOT EXISTS idx_fmr_year ON geo.fair_market_rents (year);

-- EPA Smart Location Database by census block group.
CREATE TABLE IF NOT EXISTS geo.smart_location (
    id                  BIGSERIAL PRIMARY KEY,
    geoid               TEXT NOT NULL UNIQUE,
    state_fips          TEXT,
    county_fips         TEXT,
    cbsa_name           TEXT,
    walkability_index   DOUBLE PRECISION,
    transit_freq        DOUBLE PRECISION,
    emp_density         DOUBLE PRECISION,
    hh_density          DOUBLE PRECISION,
    tot_emp             INT,
    auto_own_0_pct      DOUBLE PRECISION,
    source              TEXT NOT NULL DEFAULT 'epa_sld',
    source_id           TEXT NOT NULL,
    properties          JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_smart_location_state ON geo.smart_location (state_fips);
CREATE INDEX IF NOT EXISTS idx_smart_location_county ON geo.smart_location (county_fips);

-- Cross-DB import: CBP county employment summary.
CREATE TABLE IF NOT EXISTS geo.cbp_summary (
    id              BIGSERIAL PRIMARY KEY,
    fips            TEXT NOT NULL,
    state_fips      TEXT,
    county_name     TEXT,
    year            INT NOT NULL,
    establishments  INT,
    employees       INT,
    payroll         BIGINT,
    source          TEXT NOT NULL DEFAULT 'cbp',
    source_id       TEXT NOT NULL,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (fips, year)
);
CREATE INDEX IF NOT EXISTS idx_cbp_summary_fips ON geo.cbp_summary (fips);
CREATE INDEX IF NOT EXISTS idx_cbp_summary_year ON geo.cbp_summary (year);

-- Cross-DB import: QCEW county wage/employment summary.
CREATE TABLE IF NOT EXISTS geo.qcew_summary (
    id                  BIGSERIAL PRIMARY KEY,
    fips                TEXT NOT NULL,
    state_fips          TEXT,
    county_name         TEXT,
    year                INT NOT NULL,
    quarter             INT NOT NULL,
    avg_weekly_wage     INT,
    total_wages         BIGINT,
    month3_employment   INT,
    establishments      INT,
    source              TEXT NOT NULL DEFAULT 'qcew',
    source_id           TEXT NOT NULL,
    properties          JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (fips, year, quarter)
);
CREATE INDEX IF NOT EXISTS idx_qcew_summary_fips ON geo.qcew_summary (fips);
CREATE INDEX IF NOT EXISTS idx_qcew_summary_year ON geo.qcew_summary (year);

-- Cross-DB import: EPA facility sites.
-- Table may already exist from an earlier ArcGIS scraper migration with a different schema.
-- Add columns needed by the cross-DB import if they don't exist.
ALTER TABLE geo.epa_sites ADD COLUMN IF NOT EXISTS facility_name TEXT;
ALTER TABLE geo.epa_sites ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE geo.epa_sites ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE geo.epa_sites ADD COLUMN IF NOT EXISTS zip TEXT;
CREATE INDEX IF NOT EXISTS idx_epa_sites_state ON geo.epa_sites (state);
CREATE INDEX IF NOT EXISTS idx_epa_sites_registry ON geo.epa_sites (registry_id);

-- BLM Federal Lands (Surface Management Agency).
CREATE TABLE IF NOT EXISTS geo.federal_lands (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT,
    admin_agency    TEXT,
    state           TEXT,
    acres           DOUBLE PRECISION,
    geom            GEOMETRY(MultiPolygon, 4326),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    source          TEXT NOT NULL DEFAULT 'blm',
    source_id       TEXT NOT NULL,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_federal_lands_agency ON geo.federal_lands (admin_agency);
CREATE INDEX IF NOT EXISTS idx_federal_lands_state ON geo.federal_lands (state);
CREATE INDEX IF NOT EXISTS idx_federal_lands_geom ON geo.federal_lands USING GIST (geom);

-- BLM Mineral Leases.
CREATE TABLE IF NOT EXISTS geo.mineral_leases (
    id              BIGSERIAL PRIMARY KEY,
    lease_number    TEXT,
    lease_type      TEXT,
    commodity       TEXT,
    state           TEXT,
    acres           DOUBLE PRECISION,
    geom            GEOMETRY(MultiPolygon, 4326),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    source          TEXT NOT NULL DEFAULT 'blm',
    source_id       TEXT NOT NULL,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_mineral_leases_type ON geo.mineral_leases (lease_type);
CREATE INDEX IF NOT EXISTS idx_mineral_leases_state ON geo.mineral_leases (state);
CREATE INDEX IF NOT EXISTS idx_mineral_leases_geom ON geo.mineral_leases USING GIST (geom);

-- +goose Down
DROP TABLE IF EXISTS geo.mineral_leases;
DROP TABLE IF EXISTS geo.federal_lands;
ALTER TABLE geo.epa_sites DROP COLUMN IF EXISTS facility_name;
ALTER TABLE geo.epa_sites DROP COLUMN IF EXISTS state;
ALTER TABLE geo.epa_sites DROP COLUMN IF EXISTS city;
ALTER TABLE geo.epa_sites DROP COLUMN IF EXISTS zip;
DROP INDEX IF EXISTS geo.idx_epa_sites_state;
DROP INDEX IF EXISTS geo.idx_epa_sites_registry;
DROP TABLE IF EXISTS geo.qcew_summary;
DROP TABLE IF EXISTS geo.cbp_summary;
DROP TABLE IF EXISTS geo.smart_location;
DROP TABLE IF EXISTS geo.fair_market_rents;
DROP TABLE IF EXISTS geo.lihtc_projects;
