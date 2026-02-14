CREATE TABLE IF NOT EXISTS fed_data.f13_holdings (
    cik             VARCHAR(10) NOT NULL,
    period          DATE        NOT NULL,
    cusip           CHAR(9)     NOT NULL,
    issuer_name     VARCHAR(200),
    class_title     VARCHAR(100),
    value           BIGINT,
    shares          BIGINT,
    sh_prn_type     VARCHAR(5),
    put_call        VARCHAR(4),
    PRIMARY KEY (cik, period, cusip)
);
CREATE INDEX IF NOT EXISTS idx_f13_holdings_cusip ON fed_data.f13_holdings (cusip);
