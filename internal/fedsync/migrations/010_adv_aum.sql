CREATE TABLE IF NOT EXISTS fed_data.adv_aum (
    crd_number  INTEGER NOT NULL,
    report_date DATE    NOT NULL,
    aum         BIGINT,
    raum        BIGINT,
    num_accounts INTEGER,
    PRIMARY KEY (crd_number, report_date)
);
