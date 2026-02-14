CREATE TABLE IF NOT EXISTS fed_data.xbrl_facts (
    cik         VARCHAR(10) NOT NULL,
    fact_name   VARCHAR(100) NOT NULL,
    period_end  DATE        NOT NULL,
    value       NUMERIC,
    unit        VARCHAR(30),
    form        VARCHAR(10),
    fy          SMALLINT,
    accession   VARCHAR(25),
    PRIMARY KEY (cik, fact_name, period_end)
);
CREATE INDEX IF NOT EXISTS idx_xbrl_facts_cik ON fed_data.xbrl_facts (cik);
