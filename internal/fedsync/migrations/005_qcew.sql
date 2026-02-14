CREATE TABLE IF NOT EXISTS fed_data.qcew_data (
    area_fips       VARCHAR(5)  NOT NULL,
    own_code        CHAR(1)     NOT NULL,
    industry_code   VARCHAR(6)  NOT NULL,
    year            SMALLINT    NOT NULL,
    qtr             SMALLINT    NOT NULL,
    month1_emplvl   INTEGER,
    month2_emplvl   INTEGER,
    month3_emplvl   INTEGER,
    total_qtrly_wages BIGINT,
    avg_wkly_wage   INTEGER,
    qtrly_estabs    INTEGER,
    PRIMARY KEY (area_fips, own_code, industry_code, year, qtr)
);
CREATE INDEX IF NOT EXISTS idx_qcew_industry ON fed_data.qcew_data (industry_code);
CREATE INDEX IF NOT EXISTS idx_qcew_area ON fed_data.qcew_data (area_fips);
