CREATE TABLE IF NOT EXISTS fed_data.susb_data (
    year        SMALLINT    NOT NULL,
    fips_state  CHAR(2)     NOT NULL,
    naics       VARCHAR(6)  NOT NULL,
    entrsizedscr VARCHAR(60),
    firm        INTEGER,
    estb        INTEGER,
    empl        INTEGER,
    payr        BIGINT,
    PRIMARY KEY (year, fips_state, naics, entrsizedscr)
);
CREATE INDEX IF NOT EXISTS idx_susb_naics ON fed_data.susb_data (naics);
