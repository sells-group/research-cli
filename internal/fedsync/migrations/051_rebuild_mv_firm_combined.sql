-- 051: Rebuild mv_firm_combined to include adv_firm_details and enrichment data.
-- The original view (040) only joined adv_firms + brokercheck + entity_xref + edgar_entities.
-- This adds: firm details (services, compensation, affiliations, custody, DRP),
-- brochure enrichment (strategies, specializations), and CRS enrichment (firm type).

DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_firm_combined;

CREATE MATERIALIZED VIEW fed_data.mv_firm_combined AS
SELECT
    -- Core firm data (adv_firms)
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

    -- Firm details (adv_firm_details)
    fd.legal_name,
    fd.form_of_org,
    fd.total_employees AS detail_total_employees,
    fd.client_types,
    fd.comp_pct_aum,
    fd.comp_hourly,
    fd.comp_fixed,
    fd.comp_commissions,
    fd.comp_performance,
    fd.aum_discretionary,
    fd.aum_non_discretionary,
    fd.svc_financial_planning,
    fd.svc_portfolio_individuals,
    fd.svc_portfolio_pooled,
    fd.svc_portfolio_institutional,
    fd.svc_pension_consulting,
    fd.svc_adviser_selection,
    fd.wrap_fee_program,
    fd.financial_planning_clients,
    -- Business activities
    fd.biz_broker_dealer,
    fd.biz_insurance,
    fd.biz_real_estate,
    fd.biz_accountant,
    -- Financial affiliations
    fd.aff_broker_dealer,
    fd.aff_bank,
    fd.aff_insurance,
    -- Registration (Item 2)
    fd.sec_registered,
    fd.exempt_reporting,
    fd.state_registered,
    -- Custody (Item 9)
    fd.custody_client_cash,
    fd.custody_client_securities,
    -- DRP (Item 11)
    fd.has_any_drp,
    fd.drp_criminal_firm,
    fd.drp_regulatory_firm,

    -- BrokerCheck
    bc.num_branch_offices,
    bc.num_registered_reps,
    bc.registration_status AS brokercheck_status,

    -- Entity cross-reference
    ex.cik,
    ex.match_type AS xref_match_type,
    ex.confidence AS xref_confidence,

    -- EDGAR entity
    ee.sic,
    ee.sic_description,
    ee.tickers,
    ee.exchanges,

    -- Brochure enrichment (latest per firm via DISTINCT ON)
    be.investment_strategies,
    be.industry_specializations,
    be.min_account_size,
    be.fee_schedule,
    be.target_clients,

    -- CRS enrichment (latest per firm via DISTINCT ON)
    ce.firm_type AS crs_firm_type,
    ce.key_services AS crs_key_services,
    ce.has_disciplinary_history AS crs_disciplinary_history

FROM fed_data.adv_firms af
LEFT JOIN fed_data.adv_firm_details fd ON fd.crd_number = af.crd_number
LEFT JOIN fed_data.brokercheck bc ON bc.crd_number = af.crd_number
LEFT JOIN fed_data.entity_xref ex ON ex.crd_number = af.crd_number AND ex.confidence >= 0.7
LEFT JOIN fed_data.edgar_entities ee ON ee.cik = ex.cik
LEFT JOIN LATERAL (
    SELECT investment_strategies, industry_specializations, min_account_size,
           fee_schedule, target_clients
    FROM fed_data.adv_brochure_enrichment
    WHERE crd_number = af.crd_number
    ORDER BY enriched_at DESC
    LIMIT 1
) be ON true
LEFT JOIN LATERAL (
    SELECT firm_type, key_services, has_disciplinary_history
    FROM fed_data.adv_crs_enrichment
    WHERE crd_number = af.crd_number
    ORDER BY enriched_at DESC
    LIMIT 1
) ce ON true
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_firm_combined_crd ON fed_data.mv_firm_combined (crd_number);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_state ON fed_data.mv_firm_combined (state);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_aum ON fed_data.mv_firm_combined (aum DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_drp ON fed_data.mv_firm_combined (has_any_drp) WHERE has_any_drp = true;
