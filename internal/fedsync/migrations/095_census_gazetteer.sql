-- 095: Add geographic metadata columns to fips_codes for Census Gazetteer data.
-- Supports both state-level (fips_county='000') and county-level rows.

ALTER TABLE fed_data.fips_codes
    ADD COLUMN IF NOT EXISTS ansi_code   VARCHAR(10),
    ADD COLUMN IF NOT EXISTS aland       BIGINT,
    ADD COLUMN IF NOT EXISTS awater      BIGINT,
    ADD COLUMN IF NOT EXISTS aland_sqmi  NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS awater_sqmi NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS intptlat    NUMERIC(11,7),
    ADD COLUMN IF NOT EXISTS intptlong   NUMERIC(12,7),
    ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fips_geoid
    ON fed_data.fips_codes ((fips_state || fips_county));
