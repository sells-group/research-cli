CREATE TABLE IF NOT EXISTS fed_data.fpds_contracts (
    contract_id         VARCHAR(50) PRIMARY KEY,
    piid                VARCHAR(50),
    agency_id           VARCHAR(4),
    agency_name         VARCHAR(200),
    vendor_name         VARCHAR(200),
    vendor_duns         VARCHAR(13),
    vendor_uei          VARCHAR(12),
    vendor_city         VARCHAR(100),
    vendor_state        CHAR(2),
    vendor_zip          VARCHAR(10),
    naics               VARCHAR(6),
    psc                 VARCHAR(4),
    date_signed         DATE,
    dollars_obligated   NUMERIC(15,2),
    description         TEXT,
    updated_at          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fpds_vendor_uei ON fed_data.fpds_contracts (vendor_uei);
CREATE INDEX IF NOT EXISTS idx_fpds_naics ON fed_data.fpds_contracts (naics);
CREATE INDEX IF NOT EXISTS idx_fpds_date ON fed_data.fpds_contracts (date_signed);
CREATE INDEX IF NOT EXISTS idx_fpds_vendor_name ON fed_data.fpds_contracts USING gin (vendor_name gin_trgm_ops);
