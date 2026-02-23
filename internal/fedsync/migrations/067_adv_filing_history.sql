-- Historical filing materialized view for AUM/growth time series.
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_adv_filing_history AS
SELECT
    crd_number,
    filing_date,
    aum_total,
    aum_discretionary,
    num_accounts,
    total_employees,
    num_adviser_reps,
    LAG(aum_total) OVER w AS prior_aum,
    LAG(num_accounts) OVER w AS prior_accounts,
    LAG(total_employees) OVER w AS prior_employees,
    LAG(filing_date) OVER w AS prior_filing_date
FROM fed_data.adv_filings
WHERE filing_date IS NOT NULL
WINDOW w AS (PARTITION BY crd_number ORDER BY filing_date)
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mv_filing_history_crd
    ON fed_data.mv_adv_filing_history (crd_number, filing_date);
