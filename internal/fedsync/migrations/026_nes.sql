CREATE TABLE IF NOT EXISTS fed_data.nes_data (
    year        SMALLINT    NOT NULL,
    naics       VARCHAR(6)  NOT NULL,
    geo_id      VARCHAR(15) NOT NULL,
    firmpdemp   INTEGER,
    rcppdemp    BIGINT,
    payann_pct  NUMERIC(8,2),
    PRIMARY KEY (year, naics, geo_id)
);
