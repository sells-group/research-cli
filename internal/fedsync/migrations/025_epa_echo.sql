CREATE TABLE IF NOT EXISTS fed_data.epa_facilities (
    registry_id     VARCHAR(20) PRIMARY KEY,
    fac_name        VARCHAR(300),
    fac_city        VARCHAR(100),
    fac_state       CHAR(2),
    fac_zip         VARCHAR(10),
    naics_codes     TEXT[],
    sic_codes       TEXT[],
    fac_lat         NUMERIC(9,6),
    fac_long        NUMERIC(9,6),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_epa_name ON fed_data.epa_facilities USING gin (fac_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_epa_state ON fed_data.epa_facilities (fac_state);
