CREATE TABLE IF NOT EXISTS fed_data.cbp_data (
    year        SMALLINT    NOT NULL,
    fips_state  CHAR(2)     NOT NULL,
    fips_county CHAR(3)     NOT NULL,
    naics       VARCHAR(6)  NOT NULL,
    emp         INTEGER,
    emp_nf      CHAR(1),
    qp1         BIGINT,
    qp1_nf      CHAR(1),
    ap          BIGINT,
    ap_nf       CHAR(1),
    est         INTEGER,
    PRIMARY KEY (year, fips_state, fips_county, naics)
);
CREATE INDEX IF NOT EXISTS idx_cbp_naics ON fed_data.cbp_data (naics);
CREATE INDEX IF NOT EXISTS idx_cbp_fips ON fed_data.cbp_data (fips_state, fips_county);
