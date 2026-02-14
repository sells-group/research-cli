CREATE TABLE IF NOT EXISTS fed_data.economic_census (
    year        SMALLINT    NOT NULL,
    geo_id      VARCHAR(15) NOT NULL,
    naics       VARCHAR(6)  NOT NULL,
    estab       INTEGER,
    rcptot      BIGINT,
    payann      BIGINT,
    emp         INTEGER,
    PRIMARY KEY (year, geo_id, naics)
);
CREATE INDEX IF NOT EXISTS idx_econcensus_naics ON fed_data.economic_census (naics);
