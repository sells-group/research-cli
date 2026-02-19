CREATE TABLE IF NOT EXISTS fed_data.ppp_loans (
    loannumber              BIGINT PRIMARY KEY,
    borrowername            TEXT NOT NULL,
    borroweraddress         TEXT,
    borrowercity            TEXT,
    borrowerstate           CHAR(2),
    borrowerzip             TEXT,
    currentapprovalamount   NUMERIC,
    forgivenessamount       NUMERIC,
    jobsreported            INTEGER,
    dateapproved            DATE,
    loanstatus              TEXT,
    businesstype            TEXT,
    naicscode               VARCHAR(6),
    businessagedescription  TEXT
);

CREATE INDEX IF NOT EXISTS idx_ppp_state ON fed_data.ppp_loans (borrowerstate);
CREATE INDEX IF NOT EXISTS idx_ppp_name_upper ON fed_data.ppp_loans (UPPER(TRIM(borrowername)));
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_ppp_name_trgm ON fed_data.ppp_loans USING gin (borrowername gin_trgm_ops);
