CREATE TABLE IF NOT EXISTS fed_data.form_d (
    accession_number VARCHAR(25) PRIMARY KEY,
    cik              VARCHAR(10),
    entity_name      VARCHAR(200),
    entity_type      VARCHAR(50),
    year_of_inc      VARCHAR(4),
    state_of_inc     CHAR(2),
    industry_group   VARCHAR(100),
    revenue_range    VARCHAR(50),
    total_offering   BIGINT,
    total_sold       BIGINT,
    filing_date      DATE,
    updated_at       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_form_d_cik ON fed_data.form_d (cik);
CREATE INDEX IF NOT EXISTS idx_form_d_name ON fed_data.form_d USING gin (entity_name gin_trgm_ops);
