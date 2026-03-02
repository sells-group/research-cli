-- DOL Form 5500 (ERISA) retirement plan filings from EFAST2 FOIA bulk download.
-- Main form filings (F_5500 + F_5500_SF combined).
CREATE TABLE IF NOT EXISTS fed_data.form_5500 (
    ack_id                TEXT PRIMARY KEY,
    plan_year             SMALLINT,
    ein                   VARCHAR(9),
    plan_num              VARCHAR(3),
    sponsor_name          TEXT,
    sponsor_dba           TEXT,
    sponsor_state         CHAR(2),
    sponsor_zip           VARCHAR(10),
    plan_name             TEXT,
    type_pension          BOOLEAN,
    type_welfare          BOOLEAN,
    total_participants_boy INTEGER,
    total_participants_eoy INTEGER,
    total_assets_boy      NUMERIC,
    total_assets_eoy      NUMERIC,
    net_assets_eoy        NUMERIC,
    is_short_form         BOOLEAN NOT NULL DEFAULT FALSE,
    date_received         DATE
);

CREATE INDEX IF NOT EXISTS idx_form_5500_ein ON fed_data.form_5500 (ein);
CREATE INDEX IF NOT EXISTS idx_form_5500_plan_year ON fed_data.form_5500 (plan_year);
CREATE INDEX IF NOT EXISTS idx_form_5500_assets_eoy ON fed_data.form_5500 (total_assets_eoy);
CREATE INDEX IF NOT EXISTS idx_form_5500_sponsor_upper ON fed_data.form_5500 (UPPER(TRIM(sponsor_name)));
CREATE INDEX IF NOT EXISTS idx_form_5500_sponsor_trgm ON fed_data.form_5500 USING gin (sponsor_name gin_trgm_ops);

-- Schedule C service providers (which RIAs/BDs service which plans).
CREATE TABLE IF NOT EXISTS fed_data.form_5500_providers (
    ack_id                TEXT NOT NULL,
    row_order             SMALLINT NOT NULL,
    provider_name         TEXT,
    provider_ein          VARCHAR(9),
    provider_relation     TEXT,
    direct_compensation   NUMERIC,
    indirect_compensation NUMERIC,
    PRIMARY KEY (ack_id, row_order)
);

CREATE INDEX IF NOT EXISTS idx_form_5500_providers_ein ON fed_data.form_5500_providers (provider_ein);
CREATE INDEX IF NOT EXISTS idx_form_5500_providers_name_trgm ON fed_data.form_5500_providers USING gin (provider_name gin_trgm_ops);
