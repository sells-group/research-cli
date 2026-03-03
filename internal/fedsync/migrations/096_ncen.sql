-- N-CEN (SEC Form N-CEN) — annual census of registered investment companies.
-- Source: https://www.sec.gov/files/dera/data/form-n-cen-data-sets/
-- ZIP per quarter containing tab-delimited TSV files.

-- Registrant-level: one row per investment company per filing.
CREATE TABLE IF NOT EXISTS fed_data.ncen_registrants (
    accession_number              TEXT NOT NULL,
    cik                           TEXT NOT NULL,
    registrant_name               TEXT,
    file_num                      TEXT,
    lei                           TEXT,
    address1                      TEXT,
    address2                      TEXT,
    city                          TEXT,
    state                         TEXT,
    country                       TEXT,
    zip                           TEXT,
    phone                         TEXT,
    investment_company_type       TEXT,
    total_series                  INTEGER,
    filing_date                   DATE,
    report_ending_period          DATE,
    is_first_filing               BOOLEAN,
    is_last_filing                BOOLEAN,
    family_investment_company_name TEXT,
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_number)
);
CREATE INDEX IF NOT EXISTS idx_ncen_registrants_cik ON fed_data.ncen_registrants (cik);

-- Fund-level: one row per series per filing.
CREATE TABLE IF NOT EXISTS fed_data.ncen_funds (
    fund_id              TEXT NOT NULL,
    accession_number     TEXT NOT NULL,
    fund_name            TEXT,
    series_id            TEXT,
    lei                  TEXT,
    is_etf               BOOLEAN,
    is_index             BOOLEAN,
    is_money_market      BOOLEAN,
    is_target_date       BOOLEAN,
    is_fund_of_fund      BOOLEAN,
    monthly_avg_net_assets NUMERIC,
    daily_avg_net_assets NUMERIC,
    nav_per_share        NUMERIC,
    management_fee       NUMERIC,
    net_operating_expenses NUMERIC,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fund_id)
);
CREATE INDEX IF NOT EXISTS idx_ncen_funds_accession ON fed_data.ncen_funds (accession_number);
CREATE INDEX IF NOT EXISTS idx_ncen_funds_series ON fed_data.ncen_funds (series_id);

-- Service providers: one row per adviser per fund per filing.
CREATE TABLE IF NOT EXISTS fed_data.ncen_advisers (
    fund_id          TEXT NOT NULL,
    adviser_name     TEXT,
    adviser_crd      TEXT,
    adviser_lei      TEXT,
    file_num         TEXT,
    adviser_type     TEXT,
    state            TEXT,
    country          TEXT,
    is_affiliated    BOOLEAN,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ncen_advisers_fund ON fed_data.ncen_advisers (fund_id);
CREATE INDEX IF NOT EXISTS idx_ncen_advisers_crd ON fed_data.ncen_advisers (adviser_crd);
