-- 040_views.sql: Materialized views for common cross-dataset queries.

-- Combined firm view: ADV data enriched with EDGAR, BrokerCheck, and entity xref.
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_firm_combined AS
SELECT
    af.crd_number,
    af.firm_name,
    af.sec_number,
    af.city,
    af.state,
    af.website,
    af.aum,
    af.num_accounts,
    af.num_employees,
    af.filing_date AS adv_filing_date,
    bc.num_branch_offices,
    bc.num_registered_reps,
    bc.registration_status AS brokercheck_status,
    ex.cik,
    ex.match_type AS xref_match_type,
    ex.confidence AS xref_confidence,
    ee.sic,
    ee.sic_description,
    ee.tickers,
    ee.exchanges
FROM fed_data.adv_firms af
LEFT JOIN fed_data.brokercheck bc ON bc.crd_number = af.crd_number
LEFT JOIN fed_data.entity_xref ex ON ex.crd_number = af.crd_number AND ex.confidence >= 0.7
LEFT JOIN fed_data.edgar_entities ee ON ee.cik = ex.cik
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_firm_combined_crd ON fed_data.mv_firm_combined (crd_number);

-- Market size by NAICS: combines CBP employment/establishment data with QCEW wage data.
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_market_size AS
SELECT
    c.naics,
    c.year,
    c.fips_state,
    SUM(c.emp) AS total_emp,
    SUM(c.est) AS total_est,
    SUM(c.ap) AS total_payroll,
    q.avg_wkly_wage AS qcew_avg_weekly_wage,
    q.total_qtrly_wages AS qcew_qtrly_wages
FROM fed_data.cbp_data c
LEFT JOIN fed_data.qcew_data q
    ON q.area_fips = c.fips_state || '000'
    AND q.industry_code = c.naics
    AND q.year = c.year
    AND q.qtr = 1
    AND q.own_code = '5'
WHERE c.fips_county = '000'
GROUP BY c.naics, c.year, c.fips_state, q.avg_wkly_wage, q.total_qtrly_wages
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mv_market_naics ON fed_data.mv_market_size (naics, year);

-- 13F top holders: most recent quarter holdings by value.
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_13f_top_holders AS
SELECT
    f.cik,
    f.company_name,
    f.total_value,
    f.period_of_report,
    COUNT(h.cusip) AS num_positions,
    SUM(h.value) AS holdings_value
FROM fed_data.f13_filers f
JOIN fed_data.f13_holdings h ON h.cik = f.cik AND h.period = f.period_of_report
GROUP BY f.cik, f.company_name, f.total_value, f.period_of_report
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mv_13f_value ON fed_data.mv_13f_top_holders (total_value DESC);
