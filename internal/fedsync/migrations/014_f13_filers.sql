CREATE TABLE IF NOT EXISTS fed_data.f13_filers (
    cik             VARCHAR(10) PRIMARY KEY,
    company_name    VARCHAR(200) NOT NULL,
    form_type       VARCHAR(10),
    filing_date     DATE,
    period_of_report DATE,
    total_value     BIGINT,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_f13_filers_name ON fed_data.f13_filers USING gin (company_name gin_trgm_ops);
