-- Fund performance tracking table for cross-referencing with 13F holdings.
CREATE TABLE IF NOT EXISTS fed_data.adv_fund_performance (
    crd_number    INTEGER NOT NULL,
    fund_id       VARCHAR(50) NOT NULL,
    metric_type   VARCHAR(50) NOT NULL,
    period        VARCHAR(20),
    value_pct     NUMERIC(8,4),
    source        VARCHAR(20),
    extracted_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, fund_id, metric_type, period)
);

CREATE INDEX IF NOT EXISTS idx_fund_perf_crd
    ON fed_data.adv_fund_performance (crd_number);
