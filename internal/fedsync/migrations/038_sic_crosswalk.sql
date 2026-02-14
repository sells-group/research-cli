-- 038_sic_crosswalk.sql: SIC to NAICS crosswalk reference table.
CREATE TABLE IF NOT EXISTS fed_data.sic_crosswalk (
    sic_code        CHAR(4)     NOT NULL,
    sic_description VARCHAR(200),
    naics_code      VARCHAR(6)  NOT NULL,
    naics_description VARCHAR(300),
    PRIMARY KEY (sic_code, naics_code)
);

CREATE INDEX IF NOT EXISTS idx_sic_xwalk_sic ON fed_data.sic_crosswalk (sic_code);
CREATE INDEX IF NOT EXISTS idx_sic_xwalk_naics ON fed_data.sic_crosswalk (naics_code);
