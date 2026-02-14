-- 037_fips_codes.sql: Reference table for FIPS geographic codes.
CREATE TABLE IF NOT EXISTS fed_data.fips_codes (
    fips_state  CHAR(2)     NOT NULL,
    fips_county CHAR(3)     NOT NULL DEFAULT '000',
    state_name  VARCHAR(50),
    county_name VARCHAR(100),
    state_abbr  CHAR(2),
    PRIMARY KEY (fips_state, fips_county)
);

CREATE INDEX IF NOT EXISTS idx_fips_state ON fed_data.fips_codes (fips_state);
CREATE INDEX IF NOT EXISTS idx_fips_abbr ON fed_data.fips_codes (state_abbr);
