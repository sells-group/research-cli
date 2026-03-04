-- schema/fed_data/views.sql
-- Federal data views (regular and materialized)

-- =============================================================================
-- Regular views
-- =============================================================================

CREATE VIEW fed_data.v_custodian_advisors AS
SELECT cr.custodian_name,
    cr.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.num_accounts,
    f.state
FROM ((fed_data.adv_custodian_relationships cr
    JOIN fed_data.adv_firms f ON ((f.crd_number = cr.crd_number)))
    LEFT JOIN LATERAL ( SELECT fi2.aum_total,
            fi2.num_accounts
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = cr.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true));

CREATE VIEW fed_data.v_custodian_market_share AS
SELECT cr.custodian_name,
    count(DISTINCT cr.crd_number) AS advisor_count,
    sum(fi.aum_total) AS total_aum,
    avg(fi.aum_total) AS avg_aum
FROM (fed_data.adv_custodian_relationships cr
    LEFT JOIN LATERAL ( SELECT fi2.aum_total
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = cr.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true))
GROUP BY cr.custodian_name;

CREATE VIEW fed_data.v_service_provider_network AS
SELECT sp.provider_name,
    sp.provider_type,
    count(DISTINCT sp.crd_number) AS advisor_count,
    array_agg(DISTINCT f.firm_name ORDER BY f.firm_name) AS advisor_names
FROM (fed_data.adv_service_providers sp
    JOIN fed_data.adv_firms f ON ((f.crd_number = sp.crd_number)))
GROUP BY sp.provider_name, sp.provider_type;

-- =============================================================================
-- Materialized views
-- =============================================================================

CREATE MATERIALIZED VIEW fed_data.mv_firm_combined AS
SELECT af.crd_number,
    af.firm_name,
    af.sec_number,
    af.city,
    af.state,
    af.website,
    lf.filing_date AS adv_filing_date,
    lf.aum,
    lf.num_accounts,
    lf.num_employees,
    lf.legal_name,
    lf.form_of_org,
    lf.total_employees AS detail_total_employees,
    lf.client_types,
    lf.comp_pct_aum,
    lf.comp_hourly,
    lf.comp_fixed,
    lf.comp_commissions,
    lf.comp_performance,
    lf.aum_discretionary,
    lf.aum_non_discretionary,
    lf.svc_financial_planning,
    lf.svc_portfolio_individuals,
    lf.svc_portfolio_pooled,
    lf.svc_portfolio_institutional,
    lf.svc_pension_consulting,
    lf.svc_adviser_selection,
    lf.wrap_fee_program,
    lf.financial_planning_clients,
    lf.biz_broker_dealer,
    lf.biz_insurance,
    lf.biz_real_estate,
    lf.biz_accountant,
    lf.aff_broker_dealer,
    lf.aff_bank,
    lf.aff_insurance,
    lf.sec_registered,
    lf.exempt_reporting,
    lf.state_registered,
    lf.custody_client_cash,
    lf.custody_client_securities,
    lf.has_any_drp,
    lf.drp_criminal_firm,
    lf.drp_regulatory_firm,
    bc.num_branch_offices,
    bc.num_registered_reps,
    bc.registration_status AS brokercheck_status,
    ex.cik,
    ex.match_type AS xref_match_type,
    ex.confidence AS xref_confidence,
    ee.sic,
    ee.sic_description,
    ee.tickers,
    ee.exchanges,
    be.investment_strategies,
    be.industry_specializations,
    be.min_account_size,
    be.fee_schedule,
    be.target_clients,
    ce.firm_type AS crs_firm_type,
    ce.key_services AS crs_key_services,
    ce.has_disciplinary_history AS crs_disciplinary_history
FROM ((((((fed_data.adv_firms af
    LEFT JOIN LATERAL ( SELECT adv_filings.crd_number,
            adv_filings.filing_date,
            adv_filings.aum,
            adv_filings.raum,
            adv_filings.num_accounts,
            adv_filings.num_employees,
            adv_filings.legal_name,
            adv_filings.form_of_org,
            adv_filings.num_other_offices,
            adv_filings.total_employees,
            adv_filings.num_adviser_reps,
            adv_filings.client_types,
            adv_filings.comp_pct_aum,
            adv_filings.comp_hourly,
            adv_filings.comp_subscription,
            adv_filings.comp_fixed,
            adv_filings.comp_commissions,
            adv_filings.comp_performance,
            adv_filings.comp_other,
            adv_filings.aum_discretionary,
            adv_filings.aum_non_discretionary,
            adv_filings.aum_total,
            adv_filings.svc_financial_planning,
            adv_filings.svc_portfolio_individuals,
            adv_filings.svc_portfolio_inv_cos,
            adv_filings.svc_portfolio_pooled,
            adv_filings.svc_portfolio_institutional,
            adv_filings.svc_pension_consulting,
            adv_filings.svc_adviser_selection,
            adv_filings.svc_periodicals,
            adv_filings.svc_security_ratings,
            adv_filings.svc_market_timing,
            adv_filings.svc_seminars,
            adv_filings.svc_other,
            adv_filings.wrap_fee_program,
            adv_filings.wrap_fee_raum,
            adv_filings.financial_planning_clients,
            adv_filings.biz_broker_dealer,
            adv_filings.biz_registered_rep,
            adv_filings.biz_cpo_cta,
            adv_filings.biz_futures_commission,
            adv_filings.biz_real_estate,
            adv_filings.biz_insurance,
            adv_filings.biz_bank,
            adv_filings.biz_trust_company,
            adv_filings.biz_municipal_advisor,
            adv_filings.biz_swap_dealer,
            adv_filings.biz_major_swap,
            adv_filings.biz_accountant,
            adv_filings.biz_lawyer,
            adv_filings.biz_other_financial,
            adv_filings.aff_broker_dealer,
            adv_filings.aff_other_adviser,
            adv_filings.aff_municipal_advisor,
            adv_filings.aff_swap_dealer,
            adv_filings.aff_major_swap,
            adv_filings.aff_cpo_cta,
            adv_filings.aff_futures_commission,
            adv_filings.aff_bank,
            adv_filings.aff_trust_company,
            adv_filings.aff_accountant,
            adv_filings.aff_lawyer,
            adv_filings.aff_insurance,
            adv_filings.aff_pension_consultant,
            adv_filings.aff_real_estate,
            adv_filings.aff_lp_sponsor,
            adv_filings.aff_pooled_vehicle,
            adv_filings.sec_registered,
            adv_filings.exempt_reporting,
            adv_filings.state_registered,
            adv_filings.discretionary_authority,
            adv_filings.txn_proprietary_interest,
            adv_filings.txn_sells_own_securities,
            adv_filings.txn_buys_from_clients,
            adv_filings.txn_recommends_own,
            adv_filings.txn_recommends_broker,
            adv_filings.txn_agency_cross,
            adv_filings.txn_principal,
            adv_filings.txn_referral_compensation,
            adv_filings.txn_other_research,
            adv_filings.txn_revenue_sharing,
            adv_filings.custody_client_cash,
            adv_filings.custody_client_securities,
            adv_filings.custody_related_person,
            adv_filings.custody_qualified_custodian,
            adv_filings.custody_surprise_exam,
            adv_filings.drp_criminal_firm,
            adv_filings.drp_criminal_affiliate,
            adv_filings.drp_regulatory_firm,
            adv_filings.drp_regulatory_affiliate,
            adv_filings.drp_civil_firm,
            adv_filings.drp_civil_affiliate,
            adv_filings.drp_complaint_firm,
            adv_filings.drp_complaint_affiliate,
            adv_filings.drp_termination_firm,
            adv_filings.drp_termination_affiliate,
            adv_filings.drp_judgment,
            adv_filings.drp_financial_firm,
            adv_filings.drp_financial_affiliate,
            adv_filings.has_any_drp,
            adv_filings.updated_at
        FROM fed_data.adv_filings
        WHERE (adv_filings.crd_number = af.crd_number)
        ORDER BY adv_filings.filing_date DESC
        LIMIT 1) lf ON (true))
    LEFT JOIN fed_data.brokercheck bc ON ((bc.crd_number = af.crd_number)))
    LEFT JOIN fed_data.entity_xref ex ON (((ex.crd_number = af.crd_number) AND (ex.confidence >= 0.7))))
    LEFT JOIN fed_data.edgar_entities ee ON (((ee.cik)::text = (ex.cik)::text)))
    LEFT JOIN LATERAL ( SELECT adv_brochure_enrichment.investment_strategies,
            adv_brochure_enrichment.industry_specializations,
            adv_brochure_enrichment.min_account_size,
            adv_brochure_enrichment.fee_schedule,
            adv_brochure_enrichment.target_clients
        FROM fed_data.adv_brochure_enrichment
        WHERE (adv_brochure_enrichment.crd_number = af.crd_number)
        ORDER BY adv_brochure_enrichment.enriched_at DESC
        LIMIT 1) be ON (true))
    LEFT JOIN LATERAL ( SELECT adv_crs_enrichment.firm_type,
            adv_crs_enrichment.key_services,
            adv_crs_enrichment.has_disciplinary_history
        FROM fed_data.adv_crs_enrichment
        WHERE (adv_crs_enrichment.crd_number = af.crd_number)
        ORDER BY adv_crs_enrichment.enriched_at DESC
        LIMIT 1) ce ON (true));

CREATE UNIQUE INDEX idx_mv_firm_combined_crd ON fed_data.mv_firm_combined USING btree (crd_number);
CREATE INDEX idx_mv_firm_combined_state ON fed_data.mv_firm_combined USING btree (state);
CREATE INDEX idx_mv_firm_combined_aum ON fed_data.mv_firm_combined USING btree (aum DESC NULLS LAST);
CREATE INDEX idx_mv_firm_combined_drp ON fed_data.mv_firm_combined USING btree (has_any_drp) WHERE (has_any_drp = true);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fed_data.mv_market_size AS
SELECT c.naics,
    c.year,
    c.fips_state,
    sum(c.emp) AS total_emp,
    sum(c.est) AS total_est,
    sum(c.ap) AS total_payroll,
    q.avg_wkly_wage AS qcew_avg_weekly_wage,
    q.total_qtrly_wages AS qcew_qtrly_wages
FROM (fed_data.cbp_data c
    LEFT JOIN fed_data.qcew_data q ON ((((q.area_fips)::text = ((c.fips_state)::text || '000'::text)) AND ((q.industry_code)::text = (c.naics)::text) AND (q.year = c.year) AND (q.qtr = 1) AND (q.own_code = '5'::bpchar))))
WHERE (c.fips_county = '000'::bpchar)
GROUP BY c.naics, c.year, c.fips_state, q.avg_wkly_wage, q.total_qtrly_wages;

CREATE INDEX idx_mv_market_naics ON fed_data.mv_market_size USING btree (naics, year);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fed_data.mv_13f_top_holders AS
SELECT f.cik,
    f.company_name,
    f.total_value,
    f.period_of_report,
    count(h.cusip) AS num_positions,
    sum(h.value) AS holdings_value
FROM (fed_data.f13_filers f
    JOIN fed_data.f13_holdings h ON ((((h.cik)::text = (f.cik)::text) AND (h.period = f.period_of_report))))
GROUP BY f.cik, f.company_name, f.total_value, f.period_of_report;

CREATE INDEX idx_mv_13f_value ON fed_data.mv_13f_top_holders USING btree (total_value DESC);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fed_data.mv_adv_filing_history AS
SELECT crd_number,
    filing_date,
    aum_total,
    aum_discretionary,
    num_accounts,
    total_employees,
    num_adviser_reps,
    lag(aum_total) OVER w AS prior_aum,
    lag(num_accounts) OVER w AS prior_accounts,
    lag(total_employees) OVER w AS prior_employees,
    lag(filing_date) OVER w AS prior_filing_date
FROM fed_data.adv_filings
WHERE (filing_date IS NOT NULL)
WINDOW w AS (PARTITION BY crd_number ORDER BY filing_date);

CREATE INDEX idx_mv_filing_history_crd ON fed_data.mv_adv_filing_history USING btree (crd_number, filing_date);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fed_data.mv_adv_intelligence AS
SELECT f.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.aum_discretionary,
    fi.num_accounts,
    fi.total_employees,
    f.city,
    f.state,
    f.website,
    fi.filing_date,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'fee_schedule_aum_tiers'::text))) AS fee_schedule,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'max_fee_rate_pct'::text))) AS max_fee_rate,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'min_fee_rate_pct'::text))) AS min_fee_rate,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'primary_custodian'::text))) AS primary_custodian,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'firm_specialization'::text))) AS firm_specialization,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'minimum_account_size'::text))) AS minimum_account_size,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'primary_investment_approach'::text))) AS investment_approach,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'ownership_structure_detail'::text))) AS ownership_structure,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'succession_plan_disclosed'::text))) AS succession_plan,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'key_personnel_names'::text))) AS key_personnel,
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
    cm.aum_1yr_growth_pct,
    cm.aum_3yr_cagr_pct,
    cm.aum_5yr_cagr_pct,
    cm.concentration_risk_score,
    cm.key_person_dependency_score,
    cm.regulatory_risk_score,
    cm.hybrid_revenue_estimate,
    cm.estimated_operating_margin,
    cm.revenue_per_employee,
    ( SELECT count(*) AS count
        FROM fed_data.adv_private_funds pf
        WHERE (pf.crd_number = f.crd_number)) AS fund_count,
    ( SELECT sum(pf.gross_asset_value) AS sum
        FROM fed_data.adv_private_funds pf
        WHERE (pf.crd_number = f.crd_number)) AS total_fund_gav,
    ( SELECT count(*) AS count
        FROM fed_data.adv_advisor_answers a
        WHERE (a.crd_number = f.crd_number)) AS total_answers,
    ( SELECT avg(a.confidence) AS avg
        FROM fed_data.adv_advisor_answers a
        WHERE (a.crd_number = f.crd_number)) AS avg_confidence
FROM ((fed_data.adv_firms f
    LEFT JOIN LATERAL ( SELECT fi2.crd_number,
            fi2.filing_date,
            fi2.aum,
            fi2.raum,
            fi2.num_accounts,
            fi2.num_employees,
            fi2.legal_name,
            fi2.form_of_org,
            fi2.num_other_offices,
            fi2.total_employees,
            fi2.num_adviser_reps,
            fi2.client_types,
            fi2.comp_pct_aum,
            fi2.comp_hourly,
            fi2.comp_subscription,
            fi2.comp_fixed,
            fi2.comp_commissions,
            fi2.comp_performance,
            fi2.comp_other,
            fi2.aum_discretionary,
            fi2.aum_non_discretionary,
            fi2.aum_total,
            fi2.svc_financial_planning,
            fi2.svc_portfolio_individuals,
            fi2.svc_portfolio_inv_cos,
            fi2.svc_portfolio_pooled,
            fi2.svc_portfolio_institutional,
            fi2.svc_pension_consulting,
            fi2.svc_adviser_selection,
            fi2.svc_periodicals,
            fi2.svc_security_ratings,
            fi2.svc_market_timing,
            fi2.svc_seminars,
            fi2.svc_other,
            fi2.wrap_fee_program,
            fi2.wrap_fee_raum,
            fi2.financial_planning_clients,
            fi2.biz_broker_dealer,
            fi2.biz_registered_rep,
            fi2.biz_cpo_cta,
            fi2.biz_futures_commission,
            fi2.biz_real_estate,
            fi2.biz_insurance,
            fi2.biz_bank,
            fi2.biz_trust_company,
            fi2.biz_municipal_advisor,
            fi2.biz_swap_dealer,
            fi2.biz_major_swap,
            fi2.biz_accountant,
            fi2.biz_lawyer,
            fi2.biz_other_financial,
            fi2.aff_broker_dealer,
            fi2.aff_other_adviser,
            fi2.aff_municipal_advisor,
            fi2.aff_swap_dealer,
            fi2.aff_major_swap,
            fi2.aff_cpo_cta,
            fi2.aff_futures_commission,
            fi2.aff_bank,
            fi2.aff_trust_company,
            fi2.aff_accountant,
            fi2.aff_lawyer,
            fi2.aff_insurance,
            fi2.aff_pension_consultant,
            fi2.aff_real_estate,
            fi2.aff_lp_sponsor,
            fi2.aff_pooled_vehicle,
            fi2.sec_registered,
            fi2.exempt_reporting,
            fi2.state_registered,
            fi2.discretionary_authority,
            fi2.txn_proprietary_interest,
            fi2.txn_sells_own_securities,
            fi2.txn_buys_from_clients,
            fi2.txn_recommends_own,
            fi2.txn_recommends_broker,
            fi2.txn_agency_cross,
            fi2.txn_principal,
            fi2.txn_referral_compensation,
            fi2.txn_other_research,
            fi2.txn_revenue_sharing,
            fi2.custody_client_cash,
            fi2.custody_client_securities,
            fi2.custody_related_person,
            fi2.custody_qualified_custodian,
            fi2.custody_surprise_exam,
            fi2.drp_criminal_firm,
            fi2.drp_criminal_affiliate,
            fi2.drp_regulatory_firm,
            fi2.drp_regulatory_affiliate,
            fi2.drp_civil_firm,
            fi2.drp_civil_affiliate,
            fi2.drp_complaint_firm,
            fi2.drp_complaint_affiliate,
            fi2.drp_termination_firm,
            fi2.drp_termination_affiliate,
            fi2.drp_judgment,
            fi2.drp_financial_firm,
            fi2.drp_financial_affiliate,
            fi2.has_any_drp,
            fi2.updated_at,
            fi2.filing_type
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = f.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true))
    LEFT JOIN fed_data.adv_computed_metrics cm ON ((cm.crd_number = f.crd_number)))
WHERE (EXISTS ( SELECT 1
    FROM fed_data.adv_advisor_answers a
    WHERE (a.crd_number = f.crd_number)));

CREATE UNIQUE INDEX idx_mv_adv_intelligence_crd ON fed_data.mv_adv_intelligence USING btree (crd_number);
CREATE INDEX idx_mv_adv_intelligence_state ON fed_data.mv_adv_intelligence USING btree (state);
CREATE INDEX idx_mv_adv_intelligence_aum ON fed_data.mv_adv_intelligence USING btree (aum_total DESC NULLS LAST);
CREATE INDEX idx_mv_adv_intelligence_readiness ON fed_data.mv_adv_intelligence USING btree (acquisition_readiness DESC NULLS LAST);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fed_data.mv_pe_intelligence AS
SELECT pf.pe_firm_id,
    pf.firm_name,
    pf.firm_type,
    pf.website_url,
    pf.hq_city,
    pf.hq_state,
    pf.year_founded,
    pf.linkedin_url,
    pf.twitter_url,
    pf.facebook_url,
    pf.instagram_url,
    pf.youtube_url,
    pf.crunchbase_url,
    ria.ria_count,
    ria.total_ria_aum,
    ria.avg_ria_aum,
    ria.ria_states,
    array_length(ria.ria_states, 1) AS ria_state_count,
    geo.top_state,
    geo.top_state_aum_pct,
    ria.total_ria_accounts,
    ria.total_ria_employees,
    adv_agg.total_ria_revenue_estimate,
    adv_agg.avg_ria_revenue_per_client,
    adv_agg.avg_ria_operating_margin,
    adv_agg.avg_ria_aum_1yr_growth_pct,
    adv_agg.avg_ria_aum_3yr_cagr_pct,
    adv_agg.rias_with_drps,
    adv_agg.avg_regulatory_risk_score,
    adv_agg.avg_concentration_risk_score,
    adv_agg.avg_key_person_dependency,
    adv_agg.rias_discretionary_pct,
    svc_profile.most_common_services,
    comp_profile.most_common_compensation,
    top_rias.top_rias,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_firm_description'::text))) AS firm_description,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_firm_type'::text))) AS identified_firm_type,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_managing_partners'::text))) AS managing_partners,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_team_size'::text))) AS team_size,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_companies'::text))) AS portfolio_companies,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_count'::text))) AS portfolio_count,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_investment_strategy'::text))) AS investment_strategy,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_target_sectors'::text))) AS target_sectors,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_size_range'::text))) AS deal_size_range,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_total_aum'::text))) AS total_aum,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_fund_names'::text))) AS fund_names,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_exits_notable'::text))) AS notable_exits,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_acquisition_pattern'::text))) AS acquisition_pattern,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_strategic_assessment'::text))) AS strategic_assessment,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_contact_email'::text))) AS contact_email,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_contact_phone'::text))) AS contact_phone,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_recent_acquisitions'::text))) AS recent_acquisitions,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_velocity'::text))) AS deal_velocity,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_integration_approach'::text))) AS integration_approach,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_dry_powder'::text))) AS dry_powder,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_valuation_approach'::text))) AS valuation_approach,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_gap_analysis'::text))) AS portfolio_gap_analysis,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_competitive_position'::text))) AS competitive_position,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_investment_themes'::text))) AS investment_themes,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_market_views'::text))) AS market_views,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_target_profile_signals'::text))) AS target_profile_signals,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_announcements'::text))) AS deal_announcements,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_fundraise_signals'::text))) AS fundraise_signals,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_updates'::text))) AS portfolio_updates,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_hiring_expansion'::text))) AS hiring_expansion,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_thought_leadership'::text))) AS thought_leadership,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_content_recency'::text))) AS content_recency,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_competitive_intel'::text))) AS competitive_intel,
    ans_stats.answer_count,
    ans_stats.avg_confidence,
    lr.latest_run_at,
    lr.latest_run_status,
    lr.latest_cost_usd
FROM ((((((((fed_data.pe_firms pf
    LEFT JOIN LATERAL ( SELECT count(*) AS ria_count,
            COALESCE(sum(fi.aum_total), (0)::numeric) AS total_ria_aum,
            COALESCE(avg(fi.aum_total), (0)::numeric) AS avg_ria_aum,
            COALESCE(sum(fi.num_accounts), (0)::bigint) AS total_ria_accounts,
            COALESCE(sum(fi.total_employees), (0)::bigint) AS total_ria_employees,
            array_agg(DISTINCT f.state) FILTER (WHERE (f.state IS NOT NULL)) AS ria_states
        FROM ((fed_data.pe_firm_rias pr
            JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.aum_total,
                    fi2.num_accounts,
                    fi2.total_employees
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE (pr.pe_firm_id = pf.pe_firm_id)) ria ON (true))
    LEFT JOIN LATERAL ( SELECT COALESCE(sum(acm.revenue_estimate), (0)::numeric) AS total_ria_revenue_estimate,
            avg(acm.revenue_per_client) AS avg_ria_revenue_per_client,
            avg(acm.estimated_operating_margin) AS avg_ria_operating_margin,
            avg(acm.aum_1yr_growth_pct) AS avg_ria_aum_1yr_growth_pct,
            avg(acm.aum_3yr_cagr_pct) AS avg_ria_aum_3yr_cagr_pct,
            count(*) FILTER (WHERE (fi.has_any_drp = true)) AS rias_with_drps,
            avg(acm.regulatory_risk_score) AS avg_regulatory_risk_score,
            avg(acm.concentration_risk_score) AS avg_concentration_risk_score,
            avg(acm.key_person_dependency_score) AS avg_key_person_dependency,
                CASE
                    WHEN (count(*) > 0) THEN round(((100.0 * (count(*) FILTER (WHERE (fi.discretionary_authority = true)))::numeric) / (count(*))::numeric), 1)
                    ELSE NULL::numeric
                END AS rias_discretionary_pct
        FROM ((fed_data.pe_firm_rias pr
            LEFT JOIN fed_data.adv_computed_metrics acm ON ((acm.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.has_any_drp,
                    fi2.discretionary_authority
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE (pr.pe_firm_id = pf.pe_firm_id)) adv_agg ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(common.service ORDER BY common.service) AS most_common_services
        FROM ( SELECT per_ria.service,
                count(*) AS cnt
            FROM ( SELECT pr.crd_number,
                    unnest(ARRAY[
                        CASE
                            WHEN fi.svc_financial_planning THEN 'financial_planning'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_individuals THEN 'portfolio_individuals'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_inv_cos THEN 'portfolio_inv_cos'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_pooled THEN 'portfolio_pooled'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_institutional THEN 'portfolio_institutional'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_pension_consulting THEN 'pension_consulting'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_adviser_selection THEN 'adviser_selection'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_periodicals THEN 'periodicals'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_security_ratings THEN 'security_ratings'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_market_timing THEN 'market_timing'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_seminars THEN 'seminars'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_other THEN 'other_services'::text
                            ELSE NULL::text
                        END]) AS service
                FROM (fed_data.pe_firm_rias pr
                    LEFT JOIN LATERAL ( SELECT fi2.svc_financial_planning,
                            fi2.svc_portfolio_individuals,
                            fi2.svc_portfolio_inv_cos,
                            fi2.svc_portfolio_pooled,
                            fi2.svc_portfolio_institutional,
                            fi2.svc_pension_consulting,
                            fi2.svc_adviser_selection,
                            fi2.svc_periodicals,
                            fi2.svc_security_ratings,
                            fi2.svc_market_timing,
                            fi2.svc_seminars,
                            fi2.svc_other
                        FROM fed_data.adv_filings fi2
                        WHERE (fi2.crd_number = pr.crd_number)
                        ORDER BY fi2.filing_date DESC
                        LIMIT 1) fi ON (true))
                WHERE (pr.pe_firm_id = pf.pe_firm_id)) per_ria
            WHERE (per_ria.service IS NOT NULL)
            GROUP BY per_ria.service
            HAVING ((count(*))::numeric > ((( SELECT count(*) AS count
                FROM fed_data.pe_firm_rias
                WHERE (pe_firm_rias.pe_firm_id = pf.pe_firm_id)))::numeric * 0.5))) common) svc_profile ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(common.comp_type ORDER BY common.comp_type) AS most_common_compensation
        FROM ( SELECT per_ria.comp_type,
                count(*) AS cnt
            FROM ( SELECT pr.crd_number,
                    unnest(ARRAY[
                        CASE
                            WHEN fi.comp_pct_aum THEN 'pct_aum'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_hourly THEN 'hourly'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_subscription THEN 'subscription'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_fixed THEN 'fixed'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_commissions THEN 'commissions'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_performance THEN 'performance'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_other THEN 'other_comp'::text
                            ELSE NULL::text
                        END]) AS comp_type
                FROM (fed_data.pe_firm_rias pr
                    LEFT JOIN LATERAL ( SELECT fi2.comp_pct_aum,
                            fi2.comp_hourly,
                            fi2.comp_subscription,
                            fi2.comp_fixed,
                            fi2.comp_commissions,
                            fi2.comp_performance,
                            fi2.comp_other
                        FROM fed_data.adv_filings fi2
                        WHERE (fi2.crd_number = pr.crd_number)
                        ORDER BY fi2.filing_date DESC
                        LIMIT 1) fi ON (true))
                WHERE (pr.pe_firm_id = pf.pe_firm_id)) per_ria
            WHERE (per_ria.comp_type IS NOT NULL)
            GROUP BY per_ria.comp_type
            HAVING ((count(*))::numeric > ((( SELECT count(*) AS count
                FROM fed_data.pe_firm_rias
                WHERE (pe_firm_rias.pe_firm_id = pf.pe_firm_id)))::numeric * 0.5))) common) comp_profile ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(sub.ria_row) AS top_rias
        FROM ( SELECT jsonb_build_object('crd', pr.crd_number, 'name', f.firm_name, 'aum', fi.aum_total, 'state', f.state, 'employees', fi.total_employees) AS ria_row
            FROM ((fed_data.pe_firm_rias pr
                JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
                LEFT JOIN LATERAL ( SELECT fi2.aum_total,
                        fi2.total_employees
                    FROM fed_data.adv_filings fi2
                    WHERE (fi2.crd_number = pr.crd_number)
                    ORDER BY fi2.filing_date DESC
                    LIMIT 1) fi ON (true))
            WHERE (pr.pe_firm_id = pf.pe_firm_id)
            ORDER BY fi.aum_total DESC NULLS LAST
            LIMIT 5) sub) top_rias ON (true))
    LEFT JOIN LATERAL ( SELECT f.state AS top_state,
            round(((100.0 * sum(fi.aum_total)) / NULLIF(ria.total_ria_aum, (0)::numeric)), 1) AS top_state_aum_pct
        FROM ((fed_data.pe_firm_rias pr
            JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.aum_total
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE ((pr.pe_firm_id = pf.pe_firm_id) AND (f.state IS NOT NULL))
        GROUP BY f.state
        ORDER BY (sum(fi.aum_total)) DESC NULLS LAST
        LIMIT 1) geo ON (true))
    LEFT JOIN LATERAL ( SELECT count(*) AS answer_count,
            avg(pa.confidence) AS avg_confidence
        FROM fed_data.pe_answers pa
        WHERE (pa.pe_firm_id = pf.pe_firm_id)) ans_stats ON (true))
    LEFT JOIN LATERAL ( SELECT er.completed_at AS latest_run_at,
            er.status AS latest_run_status,
            er.cost_usd AS latest_cost_usd
        FROM fed_data.pe_extraction_runs er
        WHERE (er.pe_firm_id = pf.pe_firm_id)
        ORDER BY er.started_at DESC
        LIMIT 1) lr ON (true));

CREATE UNIQUE INDEX idx_mv_pe_intelligence_pk ON fed_data.mv_pe_intelligence USING btree (pe_firm_id);
CREATE INDEX idx_mv_pe_intelligence_ria_count ON fed_data.mv_pe_intelligence USING btree (ria_count DESC);
CREATE INDEX idx_mv_pe_intelligence_aum ON fed_data.mv_pe_intelligence USING btree (total_ria_aum DESC);
