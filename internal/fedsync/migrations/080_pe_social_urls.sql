-- 080: Add social media URL columns to pe_firms
-- Captures social URLs that are rejected as primary websites but still valuable for intelligence.

ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS linkedin_url   VARCHAR(500);
ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS twitter_url    VARCHAR(500);
ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS facebook_url   VARCHAR(500);
ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS instagram_url  VARCHAR(500);
ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS youtube_url    VARCHAR(500);
ALTER TABLE fed_data.pe_firms ADD COLUMN IF NOT EXISTS crunchbase_url VARCHAR(500);

-- Recreate matview with social URL columns
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_pe_intelligence;

CREATE MATERIALIZED VIEW fed_data.mv_pe_intelligence AS
SELECT
    pf.pe_firm_id,
    pf.firm_name,
    pf.firm_type,
    pf.website_url,
    pf.hq_city,
    pf.hq_state,
    pf.year_founded,

    -- Social media URLs
    pf.linkedin_url,
    pf.twitter_url,
    pf.facebook_url,
    pf.instagram_url,
    pf.youtube_url,
    pf.crunchbase_url,

    -- Basic RIA ownership stats
    ria.ria_count,
    ria.total_ria_aum,
    ria.avg_ria_aum,
    ria.ria_states,

    -- Geographic concentration
    array_length(ria.ria_states, 1) AS ria_state_count,
    geo.top_state,
    geo.top_state_aum_pct,

    -- Scale & Financials
    ria.total_ria_accounts,
    ria.total_ria_employees,
    adv_agg.total_ria_revenue_estimate,
    adv_agg.avg_ria_revenue_per_client,
    adv_agg.avg_ria_operating_margin,

    -- Growth Trends
    adv_agg.avg_ria_aum_1yr_growth_pct,
    adv_agg.avg_ria_aum_3yr_cagr_pct,

    -- Regulatory & Risk
    adv_agg.rias_with_drps,
    adv_agg.avg_regulatory_risk_score,
    adv_agg.avg_concentration_risk_score,
    adv_agg.avg_key_person_dependency,

    -- Service Profile
    adv_agg.rias_discretionary_pct,
    svc_profile.most_common_services,
    comp_profile.most_common_compensation,

    -- Top RIAs by AUM
    top_rias.top_rias,

    -- Firm Identity answers
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_firm_description') AS firm_description,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_firm_type') AS identified_firm_type,

    -- Key People answers
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_managing_partners') AS managing_partners,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_team_size') AS team_size,

    -- Portfolio & Strategy answers
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_portfolio_companies') AS portfolio_companies,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_portfolio_count') AS portfolio_count,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_investment_strategy') AS investment_strategy,
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_target_sectors') AS target_sectors,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_deal_size_range') AS deal_size_range,

    -- Fund & Financial answers
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_total_aum') AS total_aum,
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_fund_names') AS fund_names,
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_exits_notable') AS notable_exits,

    -- Synthesis answers
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_acquisition_pattern') AS acquisition_pattern,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_strategic_assessment') AS strategic_assessment,

    -- Contact
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_contact_email') AS contact_email,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_contact_phone') AS contact_phone,

    -- M&A Intelligence answers
    (SELECT pa.value FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_recent_acquisitions') AS recent_acquisitions,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_deal_velocity') AS deal_velocity,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_integration_approach') AS integration_approach,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_dry_powder') AS dry_powder,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_valuation_approach') AS valuation_approach,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_portfolio_gap_analysis') AS portfolio_gap_analysis,
    (SELECT pa.value #>> '{}' FROM fed_data.pe_answers pa WHERE pa.pe_firm_id = pf.pe_firm_id AND pa.question_key = 'pe_competitive_position') AS competitive_position,

    -- Answer stats
    ans_stats.answer_count,
    ans_stats.avg_confidence,

    -- Latest run info
    lr.latest_run_at,
    lr.latest_run_status,
    lr.latest_cost_usd

FROM fed_data.pe_firms pf

-- Basic RIA stats from latest filings
LEFT JOIN LATERAL (
    SELECT
        count(*) AS ria_count,
        COALESCE(sum(fi.aum_total), 0) AS total_ria_aum,
        COALESCE(avg(fi.aum_total), 0) AS avg_ria_aum,
        COALESCE(sum(fi.num_accounts), 0) AS total_ria_accounts,
        COALESCE(sum(fi.total_employees), 0) AS total_ria_employees,
        array_agg(DISTINCT f.state) FILTER (WHERE f.state IS NOT NULL) AS ria_states
    FROM fed_data.pe_firm_rias pr
    JOIN fed_data.adv_firms f ON f.crd_number = pr.crd_number
    LEFT JOIN LATERAL (
        SELECT aum_total, num_accounts, total_employees
        FROM fed_data.adv_filings fi2
        WHERE fi2.crd_number = pr.crd_number
        ORDER BY fi2.filing_date DESC LIMIT 1
    ) fi ON true
    WHERE pr.pe_firm_id = pf.pe_firm_id
) ria ON true

-- ADV computed metrics aggregates (growth, risk, revenue, discretionary)
LEFT JOIN LATERAL (
    SELECT
        COALESCE(sum(acm.revenue_estimate), 0) AS total_ria_revenue_estimate,
        avg(acm.revenue_per_client) AS avg_ria_revenue_per_client,
        avg(acm.estimated_operating_margin) AS avg_ria_operating_margin,
        avg(acm.aum_1yr_growth_pct) AS avg_ria_aum_1yr_growth_pct,
        avg(acm.aum_3yr_cagr_pct) AS avg_ria_aum_3yr_cagr_pct,
        count(*) FILTER (WHERE fi.has_any_drp = true) AS rias_with_drps,
        avg(acm.regulatory_risk_score) AS avg_regulatory_risk_score,
        avg(acm.concentration_risk_score) AS avg_concentration_risk_score,
        avg(acm.key_person_dependency_score) AS avg_key_person_dependency,
        CASE WHEN count(*) > 0
            THEN ROUND(100.0 * count(*) FILTER (WHERE fi.discretionary_authority = true) / count(*), 1)
            ELSE NULL
        END AS rias_discretionary_pct
    FROM fed_data.pe_firm_rias pr
    LEFT JOIN fed_data.adv_computed_metrics acm ON acm.crd_number = pr.crd_number
    LEFT JOIN LATERAL (
        SELECT has_any_drp, discretionary_authority
        FROM fed_data.adv_filings fi2
        WHERE fi2.crd_number = pr.crd_number
        ORDER BY fi2.filing_date DESC LIMIT 1
    ) fi ON true
    WHERE pr.pe_firm_id = pf.pe_firm_id
) adv_agg ON true

-- Services offered by >50% of owned RIAs
LEFT JOIN LATERAL (
    SELECT jsonb_agg(service ORDER BY service) AS most_common_services
    FROM (
        SELECT service, count(*) AS cnt
        FROM (
            SELECT pr.crd_number, unnest(ARRAY[
                CASE WHEN fi.svc_financial_planning THEN 'financial_planning' END,
                CASE WHEN fi.svc_portfolio_individuals THEN 'portfolio_individuals' END,
                CASE WHEN fi.svc_portfolio_inv_cos THEN 'portfolio_inv_cos' END,
                CASE WHEN fi.svc_portfolio_pooled THEN 'portfolio_pooled' END,
                CASE WHEN fi.svc_portfolio_institutional THEN 'portfolio_institutional' END,
                CASE WHEN fi.svc_pension_consulting THEN 'pension_consulting' END,
                CASE WHEN fi.svc_adviser_selection THEN 'adviser_selection' END,
                CASE WHEN fi.svc_periodicals THEN 'periodicals' END,
                CASE WHEN fi.svc_security_ratings THEN 'security_ratings' END,
                CASE WHEN fi.svc_market_timing THEN 'market_timing' END,
                CASE WHEN fi.svc_seminars THEN 'seminars' END,
                CASE WHEN fi.svc_other THEN 'other_services' END
            ]) AS service
            FROM fed_data.pe_firm_rias pr
            LEFT JOIN LATERAL (
                SELECT svc_financial_planning, svc_portfolio_individuals, svc_portfolio_inv_cos,
                       svc_portfolio_pooled, svc_portfolio_institutional, svc_pension_consulting,
                       svc_adviser_selection, svc_periodicals, svc_security_ratings,
                       svc_market_timing, svc_seminars, svc_other
                FROM fed_data.adv_filings fi2
                WHERE fi2.crd_number = pr.crd_number
                ORDER BY fi2.filing_date DESC LIMIT 1
            ) fi ON true
            WHERE pr.pe_firm_id = pf.pe_firm_id
        ) per_ria
        WHERE service IS NOT NULL
        GROUP BY service
        HAVING count(*) > (SELECT count(*) FROM fed_data.pe_firm_rias WHERE pe_firm_id = pf.pe_firm_id) * 0.5
    ) common
) svc_profile ON true

-- Compensation types used by >50% of owned RIAs
LEFT JOIN LATERAL (
    SELECT jsonb_agg(comp_type ORDER BY comp_type) AS most_common_compensation
    FROM (
        SELECT comp_type, count(*) AS cnt
        FROM (
            SELECT pr.crd_number, unnest(ARRAY[
                CASE WHEN fi.comp_pct_aum THEN 'pct_aum' END,
                CASE WHEN fi.comp_hourly THEN 'hourly' END,
                CASE WHEN fi.comp_subscription THEN 'subscription' END,
                CASE WHEN fi.comp_fixed THEN 'fixed' END,
                CASE WHEN fi.comp_commissions THEN 'commissions' END,
                CASE WHEN fi.comp_performance THEN 'performance' END,
                CASE WHEN fi.comp_other THEN 'other_comp' END
            ]) AS comp_type
            FROM fed_data.pe_firm_rias pr
            LEFT JOIN LATERAL (
                SELECT comp_pct_aum, comp_hourly, comp_subscription, comp_fixed,
                       comp_commissions, comp_performance, comp_other
                FROM fed_data.adv_filings fi2
                WHERE fi2.crd_number = pr.crd_number
                ORDER BY fi2.filing_date DESC LIMIT 1
            ) fi ON true
            WHERE pr.pe_firm_id = pf.pe_firm_id
        ) per_ria
        WHERE comp_type IS NOT NULL
        GROUP BY comp_type
        HAVING count(*) > (SELECT count(*) FROM fed_data.pe_firm_rias WHERE pe_firm_id = pf.pe_firm_id) * 0.5
    ) common
) comp_profile ON true

-- Top 5 RIAs by AUM
LEFT JOIN LATERAL (
    SELECT jsonb_agg(ria_row) AS top_rias
    FROM (
        SELECT jsonb_build_object(
            'crd', pr.crd_number,
            'name', f.firm_name,
            'aum', fi.aum_total,
            'state', f.state,
            'employees', fi.total_employees
        ) AS ria_row
        FROM fed_data.pe_firm_rias pr
        JOIN fed_data.adv_firms f ON f.crd_number = pr.crd_number
        LEFT JOIN LATERAL (
            SELECT aum_total, total_employees
            FROM fed_data.adv_filings fi2
            WHERE fi2.crd_number = pr.crd_number
            ORDER BY fi2.filing_date DESC LIMIT 1
        ) fi ON true
        WHERE pr.pe_firm_id = pf.pe_firm_id
        ORDER BY fi.aum_total DESC NULLS LAST
        LIMIT 5
    ) sub
) top_rias ON true

-- Top state by AUM for geographic concentration
LEFT JOIN LATERAL (
    SELECT
        f.state AS top_state,
        ROUND(100.0 * sum(fi.aum_total) / NULLIF(ria.total_ria_aum, 0), 1) AS top_state_aum_pct
    FROM fed_data.pe_firm_rias pr
    JOIN fed_data.adv_firms f ON f.crd_number = pr.crd_number
    LEFT JOIN LATERAL (
        SELECT aum_total FROM fed_data.adv_filings fi2
        WHERE fi2.crd_number = pr.crd_number
        ORDER BY fi2.filing_date DESC LIMIT 1
    ) fi ON true
    WHERE pr.pe_firm_id = pf.pe_firm_id AND f.state IS NOT NULL
    GROUP BY f.state
    ORDER BY sum(fi.aum_total) DESC NULLS LAST
    LIMIT 1
) geo ON true

LEFT JOIN LATERAL (
    SELECT
        count(*) AS answer_count,
        avg(confidence) AS avg_confidence
    FROM fed_data.pe_answers pa
    WHERE pa.pe_firm_id = pf.pe_firm_id
) ans_stats ON true

LEFT JOIN LATERAL (
    SELECT
        completed_at AS latest_run_at,
        status AS latest_run_status,
        cost_usd AS latest_cost_usd
    FROM fed_data.pe_extraction_runs er
    WHERE er.pe_firm_id = pf.pe_firm_id
    ORDER BY er.started_at DESC LIMIT 1
) lr ON true

WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_pe_intelligence_pk ON fed_data.mv_pe_intelligence (pe_firm_id);
CREATE INDEX idx_mv_pe_intelligence_ria_count ON fed_data.mv_pe_intelligence (ria_count DESC);
CREATE INDEX idx_mv_pe_intelligence_aum ON fed_data.mv_pe_intelligence (total_ria_aum DESC);
