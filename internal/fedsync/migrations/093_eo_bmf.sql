-- IRS Exempt Organizations Business Master File (EO BMF)
-- Source: https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
-- ~1.94M rows across 4 regional CSVs (eo1-eo4.csv)

CREATE TABLE IF NOT EXISTS fed_data.eo_bmf (
    ein              TEXT        PRIMARY KEY,
    name             TEXT        NOT NULL,
    ico              TEXT,           -- in-care-of name
    street           TEXT,
    city             TEXT,
    state            TEXT,
    zip              TEXT,
    group_exemption  TEXT,           -- group exemption number
    subsection       SMALLINT,       -- IRC subsection (03=501(c)(3), etc.)
    affiliation      SMALLINT,       -- 1=central, 3=independent, etc.
    classification   TEXT,           -- classification code(s)
    ruling           TEXT,           -- ruling date YYYYMM
    deductibility    SMALLINT,       -- 1=deductible, 2=not
    foundation       SMALLINT,       -- foundation code
    activity         TEXT,           -- activity codes
    organization     SMALLINT,       -- 1=corp, 2=trust, etc.
    status           SMALLINT,       -- exempt status code (01=unconditional, etc.)
    tax_period       TEXT,           -- YYYYMM most recent return
    asset_cd         SMALLINT,       -- asset range code
    income_cd        SMALLINT,       -- income range code
    filing_req_cd    SMALLINT,       -- filing requirement
    pf_filing_req_cd SMALLINT,       -- PF filing requirement
    acct_pd          SMALLINT,       -- accounting period month
    asset_amt        BIGINT,         -- actual asset amount
    income_amt       BIGINT,         -- income amount (can be negative)
    revenue_amt      BIGINT,         -- Form 990 revenue
    ntee_cd          TEXT,           -- NTEE classification code
    sort_name        TEXT,           -- secondary/sort name
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_eo_bmf_state ON fed_data.eo_bmf (state);
CREATE INDEX IF NOT EXISTS idx_eo_bmf_ntee ON fed_data.eo_bmf (ntee_cd);
CREATE INDEX IF NOT EXISTS idx_eo_bmf_subsection ON fed_data.eo_bmf (subsection);
CREATE INDEX IF NOT EXISTS idx_eo_bmf_name_trgm ON fed_data.eo_bmf USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_eo_bmf_asset_amt ON fed_data.eo_bmf (asset_amt DESC NULLS LAST);
