-- SBA 7(a) and 504 loan data from FOIA bulk download.
-- Unified table: program column distinguishes 7A vs 504.
-- Composite PK: (program, l2locid) uniquely identifies each loan.

CREATE TABLE IF NOT EXISTS fed_data.sba_loans (
    program             VARCHAR(3) NOT NULL,
    l2locid             BIGINT NOT NULL,
    PRIMARY KEY (program, l2locid),

    -- Borrower info
    borrname            TEXT NOT NULL,
    borrstreet          TEXT,
    borrcity            TEXT,
    borrstate           CHAR(2),
    borrzip             TEXT,

    -- Loan details
    grossapproval       NUMERIC,
    sbaguaranteedapproval NUMERIC,
    approvaldate        DATE,
    approvalfiscalyear  INTEGER,
    firstdisbursementdate DATE,
    terminmonths        INTEGER,
    initialinterestrate NUMERIC,
    fixedorvariableinterestind VARCHAR(1),
    naicscode           VARCHAR(6),
    naicsdescription    TEXT,
    loanstatus          TEXT,
    paidinfulldate      DATE,
    chargeoffdate       DATE,
    grosschargeoffamount NUMERIC,
    jobssupported       INTEGER,

    -- Business info
    businesstype        TEXT,
    businessage         TEXT,
    franchisecode       TEXT,
    franchisename       TEXT,
    processingmethod    TEXT,
    subprogram          TEXT,

    -- Project location
    projectcounty       TEXT,
    projectstate        CHAR(2),
    sbadistrictoffice   TEXT,
    congressionaldistrict TEXT,

    -- 7(a)-specific fields (NULL for 504)
    bankname            TEXT,
    bankfdicnumber      TEXT,
    bankncuanumber      TEXT,
    bankstreet          TEXT,
    bankcity            TEXT,
    bankstate           CHAR(2),
    bankzip             TEXT,
    revolverstatus      INTEGER,
    collateralind       VARCHAR(1),
    soldsecmrktind      VARCHAR(1),

    -- 504-specific fields (NULL for 7A)
    cdc_name            TEXT,
    cdc_street          TEXT,
    cdc_city            TEXT,
    cdc_state           CHAR(2),
    cdc_zip             TEXT,
    thirdpartylender_name  TEXT,
    thirdpartylender_city  TEXT,
    thirdpartylender_state CHAR(2),
    thirdpartydollars   NUMERIC,
    deliverymethod      TEXT,

    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for entity cross-referencing
CREATE INDEX IF NOT EXISTS idx_sba_loans_state ON fed_data.sba_loans (borrstate);
CREATE INDEX IF NOT EXISTS idx_sba_loans_name_upper ON fed_data.sba_loans (UPPER(TRIM(borrname)));
CREATE INDEX IF NOT EXISTS idx_sba_loans_name_trgm ON fed_data.sba_loans USING gin (borrname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sba_loans_naics ON fed_data.sba_loans (naicscode);
CREATE INDEX IF NOT EXISTS idx_sba_loans_program ON fed_data.sba_loans (program);
CREATE INDEX IF NOT EXISTS idx_sba_loans_fy ON fed_data.sba_loans (approvalfiscalyear);
-- FDIC linkage index for 7(a) bank cross-reference
CREATE INDEX IF NOT EXISTS idx_sba_loans_fdic ON fed_data.sba_loans (bankfdicnumber) WHERE bankfdicnumber IS NOT NULL;
