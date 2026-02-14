-- 036_naics_codes.sql: Reference table for NAICS industry codes.
CREATE TABLE IF NOT EXISTS fed_data.naics_codes (
    code        VARCHAR(6)  PRIMARY KEY,
    title       VARCHAR(300) NOT NULL,
    sector      CHAR(2)     NOT NULL,
    subsector   CHAR(3),
    industry_group CHAR(4),
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_naics_sector ON fed_data.naics_codes (sector);
