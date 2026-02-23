-- Rebuild materialized view with growth trend and new computed metric columns.
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_adv_intelligence;

CREATE MATERIALIZED VIEW fed_data.mv_adv_intelligence AS
SELECT
    f.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.aum_discretionary,
    fi.num_accounts,
    fi.total_employees,
    f.city,
    f.state,
    f.website,
    fi.filing_date,
    -- Key extracted answers (flattened for screening)
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'fee_schedule_aum_tiers') AS fee_schedule,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'max_fee_rate_pct') AS max_fee_rate,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'min_fee_rate_pct') AS min_fee_rate,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'primary_custodian') AS primary_custodian,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'firm_specialization') AS firm_specialization,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'minimum_account_size') AS minimum_account_size,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'primary_investment_approach') AS investment_approach,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'ownership_structure_detail') AS ownership_structure,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'succession_plan_disclosed') AS succession_plan,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'key_personnel_names') AS key_personnel,
    -- Computed metrics (original)
    cm.revenue_estimate AS computed_revenue,
    cm.blended_fee_rate_bps,
    cm.revenue_per_client AS computed_rev_per_client,
    cm.aum_growth_cagr_pct,
    cm.client_growth_rate_pct,
    cm.acquisition_readiness,
    cm.drp_severity,
    cm.business_complexity,
    cm.compensation_diversity,
    cm.fund_aum_pct_total,
    -- v3: Growth trend columns
    cm.aum_1yr_growth_pct,
    cm.aum_3yr_cagr_pct,
    cm.aum_5yr_cagr_pct,
    -- v3: Risk and profitability
    cm.concentration_risk_score,
    cm.key_person_dependency_score,
    cm.regulatory_risk_score,
    cm.hybrid_revenue_estimate,
    cm.estimated_operating_margin,
    cm.revenue_per_employee,
    -- Fund summary
    (SELECT count(*) FROM fed_data.adv_private_funds pf WHERE pf.crd_number = f.crd_number) AS fund_count,
    (SELECT sum(pf.gross_asset_value) FROM fed_data.adv_private_funds pf WHERE pf.crd_number = f.crd_number) AS total_fund_gav,
    -- Answer stats
    (SELECT count(*) FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number) AS total_answers,
    (SELECT avg(a.confidence) FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number) AS avg_confidence
FROM fed_data.adv_firms f
LEFT JOIN LATERAL (
    SELECT * FROM fed_data.adv_filings fi2
    WHERE fi2.crd_number = f.crd_number
    ORDER BY fi2.filing_date DESC
    LIMIT 1
) fi ON true
LEFT JOIN fed_data.adv_computed_metrics cm ON cm.crd_number = f.crd_number
WHERE EXISTS (
    SELECT 1 FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number
)
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_adv_intelligence_crd
    ON fed_data.mv_adv_intelligence (crd_number);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_aum
    ON fed_data.mv_adv_intelligence (aum_total DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_state
    ON fed_data.mv_adv_intelligence (state);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_readiness
    ON fed_data.mv_adv_intelligence (acquisition_readiness DESC NULLS LAST);
