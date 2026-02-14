CREATE TABLE IF NOT EXISTS fed_data.adv_firms (
    crd_number      INTEGER PRIMARY KEY,
    firm_name       VARCHAR(200) NOT NULL,
    sec_number      VARCHAR(20),
    city            VARCHAR(100),
    state           CHAR(2),
    country         VARCHAR(50),
    website         VARCHAR(300),
    aum             BIGINT,
    num_accounts    INTEGER,
    num_employees   INTEGER,
    filing_date     DATE,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_adv_firms_name ON fed_data.adv_firms USING gin (firm_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_adv_firms_state ON fed_data.adv_firms (state);
