CREATE TABLE IF NOT EXISTS fed_data.oews_data (
    area_code   VARCHAR(7)  NOT NULL,
    area_type   SMALLINT,
    naics       VARCHAR(6)  NOT NULL,
    occ_code    VARCHAR(7)  NOT NULL,
    year        SMALLINT    NOT NULL,
    tot_emp     INTEGER,
    h_mean      NUMERIC(10,2),
    a_mean      INTEGER,
    h_median    NUMERIC(10,2),
    a_median    INTEGER,
    PRIMARY KEY (area_code, naics, occ_code, year)
);
CREATE INDEX IF NOT EXISTS idx_oews_naics ON fed_data.oews_data (naics);
CREATE INDEX IF NOT EXISTS idx_oews_occ ON fed_data.oews_data (occ_code);
