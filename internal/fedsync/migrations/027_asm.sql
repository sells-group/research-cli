CREATE TABLE IF NOT EXISTS fed_data.asm_data (
    year        SMALLINT    NOT NULL,
    naics       VARCHAR(6)  NOT NULL,
    geo_id      VARCHAR(15) NOT NULL,
    valadd      BIGINT,
    totval_ship BIGINT,
    prodwrkrs   INTEGER,
    PRIMARY KEY (year, naics, geo_id)
);
