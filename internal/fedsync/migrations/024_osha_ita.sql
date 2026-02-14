CREATE TABLE IF NOT EXISTS fed_data.osha_inspections (
    activity_nr     BIGINT PRIMARY KEY,
    estab_name      VARCHAR(300),
    site_city       VARCHAR(100),
    site_state      CHAR(2),
    site_zip        VARCHAR(10),
    naics_code      VARCHAR(6),
    sic_code        VARCHAR(4),
    open_date       DATE,
    close_case_date DATE,
    case_type       CHAR(1),
    safety_hlth     CHAR(1),
    total_penalty   NUMERIC(12,2),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_osha_naics ON fed_data.osha_inspections (naics_code);
CREATE INDEX IF NOT EXISTS idx_osha_name ON fed_data.osha_inspections USING gin (estab_name gin_trgm_ops);
