-- +goose Up

-- CDC Social Vulnerability Index by census tract.
CREATE TABLE IF NOT EXISTS geo.svi (
    id            BIGSERIAL PRIMARY KEY,
    fips          TEXT NOT NULL,
    state_fips    TEXT,
    county_fips   TEXT,
    year          INT NOT NULL,
    rpl_themes    DOUBLE PRECISION,
    rpl_theme1    DOUBLE PRECISION,
    rpl_theme2    DOUBLE PRECISION,
    rpl_theme3    DOUBLE PRECISION,
    rpl_theme4    DOUBLE PRECISION,
    e_totpop      INT,
    e_pov150      INT,
    e_unemp       INT,
    e_hburd       INT,
    e_nohsdp      INT,
    e_uninsur     INT,
    ep_pov150     DOUBLE PRECISION,
    ep_unemp      DOUBLE PRECISION,
    ep_hburd      DOUBLE PRECISION,
    ep_nohsdp     DOUBLE PRECISION,
    ep_uninsur    DOUBLE PRECISION,
    source        TEXT NOT NULL DEFAULT 'cdc',
    source_id     TEXT NOT NULL,
    properties    JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (fips, year)
);
CREATE INDEX IF NOT EXISTS idx_svi_fips ON geo.svi (fips);
CREATE INDEX IF NOT EXISTS idx_svi_state ON geo.svi (state_fips);
CREATE INDEX IF NOT EXISTS idx_svi_year ON geo.svi (year);

-- BEA GDP/income by county.
CREATE TABLE IF NOT EXISTS fed_data.bea_regional (
    id            BIGSERIAL PRIMARY KEY,
    table_name    TEXT NOT NULL,
    geo_fips      TEXT NOT NULL,
    geo_name      TEXT,
    line_code     INT NOT NULL,
    description   TEXT,
    unit          TEXT,
    year          INT NOT NULL,
    value         DOUBLE PRECISION,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (table_name, geo_fips, line_code, year)
);
CREATE INDEX IF NOT EXISTS idx_bea_regional_fips ON fed_data.bea_regional (geo_fips);
CREATE INDEX IF NOT EXISTS idx_bea_regional_year ON fed_data.bea_regional (year);
CREATE INDEX IF NOT EXISTS idx_bea_regional_table ON fed_data.bea_regional (table_name);

-- IRS county-to-county migration flows.
CREATE TABLE IF NOT EXISTS fed_data.irs_soi_migration (
    id                   BIGSERIAL PRIMARY KEY,
    year                 INT NOT NULL,
    direction            TEXT NOT NULL,
    state_fips_origin    TEXT NOT NULL,
    county_fips_origin   TEXT NOT NULL,
    state_fips_dest      TEXT NOT NULL,
    county_fips_dest     TEXT NOT NULL,
    num_returns          INT,
    num_exemptions       INT,
    adjusted_gross_income BIGINT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (year, direction, state_fips_origin, county_fips_origin, state_fips_dest, county_fips_dest)
);
CREATE INDEX IF NOT EXISTS idx_irs_soi_migration_year ON fed_data.irs_soi_migration (year);
CREATE INDEX IF NOT EXISTS idx_irs_soi_migration_origin ON fed_data.irs_soi_migration (state_fips_origin, county_fips_origin);
CREATE INDEX IF NOT EXISTS idx_irs_soi_migration_dest ON fed_data.irs_soi_migration (state_fips_dest, county_fips_dest);

-- Census BPS annual county permits.
CREATE TABLE IF NOT EXISTS fed_data.building_permits (
    id                       BIGSERIAL PRIMARY KEY,
    year                     INT NOT NULL,
    state_fips               TEXT NOT NULL,
    county_fips              TEXT NOT NULL,
    county_name              TEXT,
    total_permits            INT,
    one_unit_permits         INT,
    two_unit_permits         INT,
    three_four_unit_permits  INT,
    five_plus_unit_permits   INT,
    total_valuation          BIGINT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (year, state_fips, county_fips)
);
CREATE INDEX IF NOT EXISTS idx_building_permits_year ON fed_data.building_permits (year);
CREATE INDEX IF NOT EXISTS idx_building_permits_fips ON fed_data.building_permits (state_fips, county_fips);

-- LEHD county-to-county commuter flows.
CREATE TABLE IF NOT EXISTS fed_data.lehd_lodes (
    id                       BIGSERIAL PRIMARY KEY,
    year                     INT NOT NULL,
    state                    TEXT NOT NULL,
    w_county_fips            TEXT NOT NULL,
    h_county_fips            TEXT NOT NULL,
    total_jobs               INT,
    jobs_age_29_or_younger   INT,
    jobs_age_30_to_54        INT,
    jobs_age_55_plus         INT,
    jobs_earn_1250_or_less   INT,
    jobs_earn_1251_to_3333   INT,
    jobs_earn_3334_or_more   INT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (year, state, w_county_fips, h_county_fips)
);
CREATE INDEX IF NOT EXISTS idx_lehd_lodes_year ON fed_data.lehd_lodes (year);
CREATE INDEX IF NOT EXISTS idx_lehd_lodes_work ON fed_data.lehd_lodes (w_county_fips);
CREATE INDEX IF NOT EXISTS idx_lehd_lodes_home ON fed_data.lehd_lodes (h_county_fips);

-- +goose Down
DROP TABLE IF EXISTS fed_data.lehd_lodes;
DROP TABLE IF EXISTS fed_data.building_permits;
DROP TABLE IF EXISTS fed_data.irs_soi_migration;
DROP TABLE IF EXISTS fed_data.bea_regional;
DROP TABLE IF EXISTS geo.svi;
