-- FDIC BankFind: institutions + branches
-- Source: https://api.fdic.gov/banks/

CREATE TABLE IF NOT EXISTS fed_data.fdic_institutions (
    -- Identity
    cert              INT PRIMARY KEY,
    name              TEXT NOT NULL,
    active            INT,
    inactive          INT,

    -- Address
    address           TEXT,
    address2          TEXT,
    city              TEXT,
    stalp             TEXT,
    stname            TEXT,
    zip               TEXT,
    county            TEXT,
    stnum             TEXT,
    stcnty            TEXT,
    latitude          NUMERIC(10,7),
    longitude         NUMERIC(10,7),

    -- CBSA / MSA geography
    cbsa              TEXT,
    cbsa_no           TEXT,
    cbsa_div          TEXT,
    cbsa_div_no       TEXT,
    cbsa_div_flg      TEXT,
    cbsa_metro        TEXT,
    cbsa_metro_flg    TEXT,
    cbsa_metro_name   TEXT,
    cbsa_micro_flg    TEXT,
    csa               TEXT,
    csa_no            TEXT,
    csa_flg           TEXT,

    -- Classification
    bkclass           TEXT,
    clcode            INT,
    specgrp           INT,
    instcat           INT,
    charter_class     TEXT,
    cb                TEXT,

    -- Regulatory
    regagnt           TEXT,
    regagent2         TEXT,
    chrtagnt          TEXT,
    charter           TEXT,
    stchrtr           TEXT,
    fedchrtr          TEXT,
    fed               TEXT,
    fed_rssd          TEXT,
    fdicdbs           TEXT,
    fdicregn          TEXT,
    fdicsupv          TEXT,
    suprv_fd          TEXT,
    occdist           TEXT,
    docket            TEXT,
    cfpbflag          TEXT,
    cfpbeffdte        TEXT,
    cfpbenddte        TEXT,

    -- Insurance
    insagnt1          TEXT,
    insagnt2          TEXT,
    insbif            TEXT,
    inscoml           TEXT,
    insdate           TEXT,
    insdif            TEXT,
    insfdic           INT,
    inssaif           TEXT,
    inssave           TEXT,

    -- Financial
    asset             BIGINT,
    dep               BIGINT,
    depdom            BIGINT,
    eq                TEXT,
    netinc            BIGINT,
    roa               NUMERIC(10,4),
    roe               NUMERIC(10,4),

    -- Operations
    offices           INT,
    offdom            INT,
    offfor            INT,
    offoa             INT,
    webaddr           TEXT,
    trust             TEXT,

    -- Dates
    estymd            TEXT,
    endefymd          TEXT,
    effdate           TEXT,
    procdate          TEXT,
    dateupdt          TEXT,
    repdte            TEXT,
    risdate           TEXT,
    rundate           TEXT,

    -- Structural changes
    changec1          TEXT,
    newcert           TEXT,
    ultcert           TEXT,
    priorname1        TEXT,

    -- Holding company
    hctmult           TEXT,
    namehcr           TEXT,
    parcert           TEXT,
    rssdhcr           TEXT,
    cityhcr           TEXT,
    stalphcr          TEXT,

    -- Special flags
    conserve          TEXT,
    mdi_status_code   TEXT,
    mdi_status_desc   TEXT,
    mutual            TEXT,
    subchaps          TEXT,
    oakar             TEXT,
    sasser            TEXT,
    law_sasser_flg    TEXT,
    iba               TEXT,
    qbprcoml          TEXT,
    denovo            TEXT,
    form31            TEXT,

    -- Additional websites & trade names
    te01n528          TEXT,
    te02n528          TEXT,
    te03n528          TEXT,
    te04n528          TEXT,
    te05n528          TEXT,
    te06n528          TEXT,
    te07n528          TEXT,
    te08n528          TEXT,
    te09n528          TEXT,
    te10n528          TEXT,
    te01n529          TEXT,
    te02n529          TEXT,
    te03n529          TEXT,
    te04n529          TEXT,
    te05n529          TEXT,
    te06n529          TEXT,

    -- Other
    uninum            TEXT,
    oi                TEXT,

    synced_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fed_data.fdic_branches (
    uni_num           INT PRIMARY KEY,
    cert              INT NOT NULL REFERENCES fed_data.fdic_institutions(cert),
    name              TEXT,
    off_name          TEXT,
    off_num           TEXT,
    fi_uninum         TEXT,

    -- Address
    address           TEXT,
    address2          TEXT,
    city              TEXT,
    stalp             TEXT,
    stname            TEXT,
    zip               TEXT,
    county            TEXT,
    stcnty            TEXT,

    -- Coordinates
    latitude          NUMERIC(10,7),
    longitude         NUMERIC(10,7),

    -- Classification
    main_off          INT,
    bk_class          TEXT,
    serv_type         INT,
    serv_type_desc    TEXT,

    -- CBSA / MSA geography
    cbsa              TEXT,
    cbsa_no           TEXT,
    cbsa_div          TEXT,
    cbsa_div_no       TEXT,
    cbsa_div_flg      TEXT,
    cbsa_metro        TEXT,
    cbsa_metro_flg    TEXT,
    cbsa_metro_name   TEXT,
    cbsa_micro_flg    TEXT,
    csa               TEXT,
    csa_no            TEXT,
    csa_flg           TEXT,

    -- Other
    mdi_status_code   TEXT,
    mdi_status_desc   TEXT,
    run_date          DATE,
    estymd            TEXT,
    acqdate           TEXT,

    synced_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fdic_inst_state ON fed_data.fdic_institutions(stalp);
CREATE INDEX IF NOT EXISTS idx_fdic_inst_active ON fed_data.fdic_institutions(active);
CREATE INDEX IF NOT EXISTS idx_fdic_inst_asset ON fed_data.fdic_institutions(asset);
CREATE INDEX IF NOT EXISTS idx_fdic_inst_cbsa ON fed_data.fdic_institutions(cbsa_no);
CREATE INDEX IF NOT EXISTS idx_fdic_inst_name ON fed_data.fdic_institutions(name);
CREATE INDEX IF NOT EXISTS idx_fdic_branches_cert ON fed_data.fdic_branches(cert);
CREATE INDEX IF NOT EXISTS idx_fdic_branches_state ON fed_data.fdic_branches(stalp);
CREATE INDEX IF NOT EXISTS idx_fdic_branches_cbsa ON fed_data.fdic_branches(cbsa_no);
CREATE INDEX IF NOT EXISTS idx_fdic_branches_coords ON fed_data.fdic_branches(latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fdic_branches_main ON fed_data.fdic_branches(cert) WHERE main_off = 1;
