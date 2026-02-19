-- 052: Restructure ADV data model — firm identity + filing history.
-- Creates adv_filings (accumulates per filing), migrates data from adv_aum + adv_firm_details,
-- slims adv_firms to identity-only, drops absorbed tables, rebuilds mv_firm_combined.

-- 1. Create adv_filings
CREATE TABLE IF NOT EXISTS fed_data.adv_filings (
    crd_number          INTEGER NOT NULL,
    filing_date         DATE NOT NULL,
    -- Metrics (from adv_aum + adv_firms)
    aum                 BIGINT,
    raum                BIGINT,
    num_accounts        INTEGER,
    num_employees       INTEGER,
    -- Detail columns (from adv_firm_details, migrations 047 + 050)
    legal_name              VARCHAR(300),
    form_of_org             VARCHAR(100),
    num_other_offices       INTEGER,
    total_employees         INTEGER,
    num_adviser_reps        INTEGER,
    client_types            JSONB,
    comp_pct_aum            BOOLEAN DEFAULT false,
    comp_hourly             BOOLEAN DEFAULT false,
    comp_subscription       BOOLEAN DEFAULT false,
    comp_fixed              BOOLEAN DEFAULT false,
    comp_commissions        BOOLEAN DEFAULT false,
    comp_performance        BOOLEAN DEFAULT false,
    comp_other              BOOLEAN DEFAULT false,
    aum_discretionary       BIGINT,
    aum_non_discretionary   BIGINT,
    aum_total               BIGINT,
    svc_financial_planning          BOOLEAN DEFAULT false,
    svc_portfolio_individuals       BOOLEAN DEFAULT false,
    svc_portfolio_inv_cos           BOOLEAN DEFAULT false,
    svc_portfolio_pooled            BOOLEAN DEFAULT false,
    svc_portfolio_institutional     BOOLEAN DEFAULT false,
    svc_pension_consulting          BOOLEAN DEFAULT false,
    svc_adviser_selection           BOOLEAN DEFAULT false,
    svc_periodicals                 BOOLEAN DEFAULT false,
    svc_security_ratings            BOOLEAN DEFAULT false,
    svc_market_timing               BOOLEAN DEFAULT false,
    svc_seminars                    BOOLEAN DEFAULT false,
    svc_other                       BOOLEAN DEFAULT false,
    wrap_fee_program        BOOLEAN DEFAULT false,
    wrap_fee_raum           BIGINT,
    financial_planning_clients INTEGER,
    biz_broker_dealer       BOOLEAN DEFAULT false,
    biz_registered_rep      BOOLEAN DEFAULT false,
    biz_cpo_cta             BOOLEAN DEFAULT false,
    biz_futures_commission  BOOLEAN DEFAULT false,
    biz_real_estate         BOOLEAN DEFAULT false,
    biz_insurance           BOOLEAN DEFAULT false,
    biz_bank                BOOLEAN DEFAULT false,
    biz_trust_company       BOOLEAN DEFAULT false,
    biz_municipal_advisor   BOOLEAN DEFAULT false,
    biz_swap_dealer         BOOLEAN DEFAULT false,
    biz_major_swap          BOOLEAN DEFAULT false,
    biz_accountant          BOOLEAN DEFAULT false,
    biz_lawyer              BOOLEAN DEFAULT false,
    biz_other_financial     BOOLEAN DEFAULT false,
    aff_broker_dealer       BOOLEAN DEFAULT false,
    aff_other_adviser       BOOLEAN DEFAULT false,
    aff_municipal_advisor   BOOLEAN DEFAULT false,
    aff_swap_dealer         BOOLEAN DEFAULT false,
    aff_major_swap          BOOLEAN DEFAULT false,
    aff_cpo_cta             BOOLEAN DEFAULT false,
    aff_futures_commission  BOOLEAN DEFAULT false,
    aff_bank                BOOLEAN DEFAULT false,
    aff_trust_company       BOOLEAN DEFAULT false,
    aff_accountant          BOOLEAN DEFAULT false,
    aff_lawyer              BOOLEAN DEFAULT false,
    aff_insurance           BOOLEAN DEFAULT false,
    aff_pension_consultant  BOOLEAN DEFAULT false,
    aff_real_estate         BOOLEAN DEFAULT false,
    aff_lp_sponsor          BOOLEAN DEFAULT false,
    aff_pooled_vehicle      BOOLEAN DEFAULT false,
    sec_registered          BOOLEAN DEFAULT false,
    exempt_reporting        BOOLEAN DEFAULT false,
    state_registered        BOOLEAN DEFAULT false,
    discretionary_authority BOOLEAN DEFAULT false,
    txn_proprietary_interest BOOLEAN DEFAULT false,
    txn_sells_own_securities BOOLEAN DEFAULT false,
    txn_buys_from_clients   BOOLEAN DEFAULT false,
    txn_recommends_own      BOOLEAN DEFAULT false,
    txn_recommends_broker   BOOLEAN DEFAULT false,
    txn_agency_cross        BOOLEAN DEFAULT false,
    txn_principal           BOOLEAN DEFAULT false,
    txn_referral_compensation BOOLEAN DEFAULT false,
    txn_other_research      BOOLEAN DEFAULT false,
    txn_revenue_sharing     BOOLEAN DEFAULT false,
    custody_client_cash     BOOLEAN DEFAULT false,
    custody_client_securities BOOLEAN DEFAULT false,
    custody_related_person  BOOLEAN DEFAULT false,
    custody_qualified_custodian BOOLEAN DEFAULT false,
    custody_surprise_exam   BOOLEAN DEFAULT false,
    drp_criminal_firm       BOOLEAN DEFAULT false,
    drp_criminal_affiliate  BOOLEAN DEFAULT false,
    drp_regulatory_firm     BOOLEAN DEFAULT false,
    drp_regulatory_affiliate BOOLEAN DEFAULT false,
    drp_civil_firm          BOOLEAN DEFAULT false,
    drp_civil_affiliate     BOOLEAN DEFAULT false,
    drp_complaint_firm      BOOLEAN DEFAULT false,
    drp_complaint_affiliate BOOLEAN DEFAULT false,
    drp_termination_firm    BOOLEAN DEFAULT false,
    drp_termination_affiliate BOOLEAN DEFAULT false,
    drp_judgment            BOOLEAN DEFAULT false,
    drp_financial_firm      BOOLEAN DEFAULT false,
    drp_financial_affiliate BOOLEAN DEFAULT false,
    has_any_drp             BOOLEAN DEFAULT false,
    updated_at              TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, filing_date)
);

-- 2. Migrate adv_aum → adv_filings
INSERT INTO fed_data.adv_filings (crd_number, filing_date, aum, raum, num_accounts)
SELECT crd_number, report_date, aum, raum, num_accounts
FROM fed_data.adv_aum
ON CONFLICT (crd_number, filing_date) DO NOTHING;

-- 3. Merge adv_firm_details into the filing row matching the firm's current filing_date.
INSERT INTO fed_data.adv_filings (
    crd_number, filing_date, num_employees,
    legal_name, form_of_org, num_other_offices, total_employees, num_adviser_reps, client_types,
    comp_pct_aum, comp_hourly, comp_subscription, comp_fixed, comp_commissions, comp_performance, comp_other,
    aum_discretionary, aum_non_discretionary, aum_total,
    svc_financial_planning, svc_portfolio_individuals, svc_portfolio_inv_cos, svc_portfolio_pooled,
    svc_portfolio_institutional, svc_pension_consulting, svc_adviser_selection, svc_periodicals,
    svc_security_ratings, svc_market_timing, svc_seminars, svc_other,
    wrap_fee_program, wrap_fee_raum, financial_planning_clients,
    biz_broker_dealer, biz_registered_rep, biz_cpo_cta, biz_futures_commission, biz_real_estate,
    biz_insurance, biz_bank, biz_trust_company, biz_municipal_advisor, biz_swap_dealer,
    biz_major_swap, biz_accountant, biz_lawyer, biz_other_financial,
    aff_broker_dealer, aff_other_adviser, aff_municipal_advisor, aff_swap_dealer, aff_major_swap,
    aff_cpo_cta, aff_futures_commission, aff_bank, aff_trust_company, aff_accountant,
    aff_lawyer, aff_insurance, aff_pension_consultant, aff_real_estate, aff_lp_sponsor, aff_pooled_vehicle,
    sec_registered, exempt_reporting, state_registered, discretionary_authority,
    txn_proprietary_interest, txn_sells_own_securities, txn_buys_from_clients, txn_recommends_own,
    txn_recommends_broker, txn_agency_cross, txn_principal, txn_referral_compensation,
    txn_other_research, txn_revenue_sharing,
    custody_client_cash, custody_client_securities, custody_related_person, custody_qualified_custodian,
    custody_surprise_exam,
    drp_criminal_firm, drp_criminal_affiliate, drp_regulatory_firm, drp_regulatory_affiliate,
    drp_civil_firm, drp_civil_affiliate, drp_complaint_firm, drp_complaint_affiliate,
    drp_termination_firm, drp_termination_affiliate, drp_judgment, drp_financial_firm,
    drp_financial_affiliate, has_any_drp
)
SELECT
    fd.crd_number, COALESCE(af.filing_date, af.updated_at::date, CURRENT_DATE), af.num_employees,
    fd.legal_name, fd.form_of_org, fd.num_other_offices, fd.total_employees, fd.num_adviser_reps, fd.client_types,
    fd.comp_pct_aum, fd.comp_hourly, fd.comp_subscription, fd.comp_fixed, fd.comp_commissions, fd.comp_performance, fd.comp_other,
    fd.aum_discretionary, fd.aum_non_discretionary, fd.aum_total,
    fd.svc_financial_planning, fd.svc_portfolio_individuals, fd.svc_portfolio_inv_cos, fd.svc_portfolio_pooled,
    fd.svc_portfolio_institutional, fd.svc_pension_consulting, fd.svc_adviser_selection, fd.svc_periodicals,
    fd.svc_security_ratings, fd.svc_market_timing, fd.svc_seminars, fd.svc_other,
    fd.wrap_fee_program, fd.wrap_fee_raum, fd.financial_planning_clients,
    fd.biz_broker_dealer, fd.biz_registered_rep, fd.biz_cpo_cta, fd.biz_futures_commission, fd.biz_real_estate,
    fd.biz_insurance, fd.biz_bank, fd.biz_trust_company, fd.biz_municipal_advisor, fd.biz_swap_dealer,
    fd.biz_major_swap, fd.biz_accountant, fd.biz_lawyer, fd.biz_other_financial,
    fd.aff_broker_dealer, fd.aff_other_adviser, fd.aff_municipal_advisor, fd.aff_swap_dealer, fd.aff_major_swap,
    fd.aff_cpo_cta, fd.aff_futures_commission, fd.aff_bank, fd.aff_trust_company, fd.aff_accountant,
    fd.aff_lawyer, fd.aff_insurance, fd.aff_pension_consultant, fd.aff_real_estate, fd.aff_lp_sponsor, fd.aff_pooled_vehicle,
    fd.sec_registered, fd.exempt_reporting, fd.state_registered, fd.discretionary_authority,
    fd.txn_proprietary_interest, fd.txn_sells_own_securities, fd.txn_buys_from_clients, fd.txn_recommends_own,
    fd.txn_recommends_broker, fd.txn_agency_cross, fd.txn_principal, fd.txn_referral_compensation,
    fd.txn_other_research, fd.txn_revenue_sharing,
    fd.custody_client_cash, fd.custody_client_securities, fd.custody_related_person, fd.custody_qualified_custodian,
    fd.custody_surprise_exam,
    fd.drp_criminal_firm, fd.drp_criminal_affiliate, fd.drp_regulatory_firm, fd.drp_regulatory_affiliate,
    fd.drp_civil_firm, fd.drp_civil_affiliate, fd.drp_complaint_firm, fd.drp_complaint_affiliate,
    fd.drp_termination_firm, fd.drp_termination_affiliate, fd.drp_judgment, fd.drp_financial_firm,
    fd.drp_financial_affiliate, fd.has_any_drp
FROM fed_data.adv_firm_details fd
JOIN fed_data.adv_firms af ON af.crd_number = fd.crd_number
ON CONFLICT (crd_number, filing_date) DO UPDATE SET
    num_employees = EXCLUDED.num_employees,
    legal_name = EXCLUDED.legal_name, form_of_org = EXCLUDED.form_of_org,
    num_other_offices = EXCLUDED.num_other_offices, total_employees = EXCLUDED.total_employees,
    num_adviser_reps = EXCLUDED.num_adviser_reps, client_types = EXCLUDED.client_types,
    comp_pct_aum = EXCLUDED.comp_pct_aum, comp_hourly = EXCLUDED.comp_hourly,
    comp_subscription = EXCLUDED.comp_subscription, comp_fixed = EXCLUDED.comp_fixed,
    comp_commissions = EXCLUDED.comp_commissions, comp_performance = EXCLUDED.comp_performance,
    comp_other = EXCLUDED.comp_other,
    aum_discretionary = EXCLUDED.aum_discretionary, aum_non_discretionary = EXCLUDED.aum_non_discretionary,
    aum_total = EXCLUDED.aum_total,
    svc_financial_planning = EXCLUDED.svc_financial_planning, svc_portfolio_individuals = EXCLUDED.svc_portfolio_individuals,
    svc_portfolio_inv_cos = EXCLUDED.svc_portfolio_inv_cos, svc_portfolio_pooled = EXCLUDED.svc_portfolio_pooled,
    svc_portfolio_institutional = EXCLUDED.svc_portfolio_institutional, svc_pension_consulting = EXCLUDED.svc_pension_consulting,
    svc_adviser_selection = EXCLUDED.svc_adviser_selection, svc_periodicals = EXCLUDED.svc_periodicals,
    svc_security_ratings = EXCLUDED.svc_security_ratings, svc_market_timing = EXCLUDED.svc_market_timing,
    svc_seminars = EXCLUDED.svc_seminars, svc_other = EXCLUDED.svc_other,
    wrap_fee_program = EXCLUDED.wrap_fee_program, wrap_fee_raum = EXCLUDED.wrap_fee_raum,
    financial_planning_clients = EXCLUDED.financial_planning_clients,
    biz_broker_dealer = EXCLUDED.biz_broker_dealer, biz_registered_rep = EXCLUDED.biz_registered_rep,
    biz_cpo_cta = EXCLUDED.biz_cpo_cta, biz_futures_commission = EXCLUDED.biz_futures_commission,
    biz_real_estate = EXCLUDED.biz_real_estate, biz_insurance = EXCLUDED.biz_insurance,
    biz_bank = EXCLUDED.biz_bank, biz_trust_company = EXCLUDED.biz_trust_company,
    biz_municipal_advisor = EXCLUDED.biz_municipal_advisor, biz_swap_dealer = EXCLUDED.biz_swap_dealer,
    biz_major_swap = EXCLUDED.biz_major_swap, biz_accountant = EXCLUDED.biz_accountant,
    biz_lawyer = EXCLUDED.biz_lawyer, biz_other_financial = EXCLUDED.biz_other_financial,
    aff_broker_dealer = EXCLUDED.aff_broker_dealer, aff_other_adviser = EXCLUDED.aff_other_adviser,
    aff_municipal_advisor = EXCLUDED.aff_municipal_advisor, aff_swap_dealer = EXCLUDED.aff_swap_dealer,
    aff_major_swap = EXCLUDED.aff_major_swap, aff_cpo_cta = EXCLUDED.aff_cpo_cta,
    aff_futures_commission = EXCLUDED.aff_futures_commission, aff_bank = EXCLUDED.aff_bank,
    aff_trust_company = EXCLUDED.aff_trust_company, aff_accountant = EXCLUDED.aff_accountant,
    aff_lawyer = EXCLUDED.aff_lawyer, aff_insurance = EXCLUDED.aff_insurance,
    aff_pension_consultant = EXCLUDED.aff_pension_consultant, aff_real_estate = EXCLUDED.aff_real_estate,
    aff_lp_sponsor = EXCLUDED.aff_lp_sponsor, aff_pooled_vehicle = EXCLUDED.aff_pooled_vehicle,
    sec_registered = EXCLUDED.sec_registered, exempt_reporting = EXCLUDED.exempt_reporting,
    state_registered = EXCLUDED.state_registered, discretionary_authority = EXCLUDED.discretionary_authority,
    txn_proprietary_interest = EXCLUDED.txn_proprietary_interest, txn_sells_own_securities = EXCLUDED.txn_sells_own_securities,
    txn_buys_from_clients = EXCLUDED.txn_buys_from_clients, txn_recommends_own = EXCLUDED.txn_recommends_own,
    txn_recommends_broker = EXCLUDED.txn_recommends_broker, txn_agency_cross = EXCLUDED.txn_agency_cross,
    txn_principal = EXCLUDED.txn_principal, txn_referral_compensation = EXCLUDED.txn_referral_compensation,
    txn_other_research = EXCLUDED.txn_other_research, txn_revenue_sharing = EXCLUDED.txn_revenue_sharing,
    custody_client_cash = EXCLUDED.custody_client_cash, custody_client_securities = EXCLUDED.custody_client_securities,
    custody_related_person = EXCLUDED.custody_related_person, custody_qualified_custodian = EXCLUDED.custody_qualified_custodian,
    custody_surprise_exam = EXCLUDED.custody_surprise_exam,
    drp_criminal_firm = EXCLUDED.drp_criminal_firm, drp_criminal_affiliate = EXCLUDED.drp_criminal_affiliate,
    drp_regulatory_firm = EXCLUDED.drp_regulatory_firm, drp_regulatory_affiliate = EXCLUDED.drp_regulatory_affiliate,
    drp_civil_firm = EXCLUDED.drp_civil_firm, drp_civil_affiliate = EXCLUDED.drp_civil_affiliate,
    drp_complaint_firm = EXCLUDED.drp_complaint_firm, drp_complaint_affiliate = EXCLUDED.drp_complaint_affiliate,
    drp_termination_firm = EXCLUDED.drp_termination_firm, drp_termination_affiliate = EXCLUDED.drp_termination_affiliate,
    drp_judgment = EXCLUDED.drp_judgment, drp_financial_firm = EXCLUDED.drp_financial_firm,
    drp_financial_affiliate = EXCLUDED.drp_financial_affiliate, has_any_drp = EXCLUDED.has_any_drp;

-- 4. Drop MV before altering adv_firms
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_firm_combined;

-- 5. Slim adv_firms to identity-only
ALTER TABLE fed_data.adv_firms DROP COLUMN IF EXISTS aum;
ALTER TABLE fed_data.adv_firms DROP COLUMN IF EXISTS num_accounts;
ALTER TABLE fed_data.adv_firms DROP COLUMN IF EXISTS num_employees;
ALTER TABLE fed_data.adv_firms DROP COLUMN IF EXISTS filing_date;

-- 6. Drop absorbed tables
DROP TABLE IF EXISTS fed_data.adv_aum;
DROP TABLE IF EXISTS fed_data.adv_firm_details;

-- 7. Indexes on adv_filings
CREATE INDEX IF NOT EXISTS idx_adv_filings_date ON fed_data.adv_filings (filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_adv_filings_aum ON fed_data.adv_filings (aum DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_adv_filings_drp ON fed_data.adv_filings (has_any_drp) WHERE has_any_drp = true;

-- 8. Rebuild mv_firm_combined with LATERAL join to latest filing
CREATE MATERIALIZED VIEW fed_data.mv_firm_combined AS
SELECT
    af.crd_number, af.firm_name, af.sec_number, af.city, af.state, af.website,
    lf.filing_date AS adv_filing_date, lf.aum, lf.num_accounts, lf.num_employees,
    lf.legal_name, lf.form_of_org,
    lf.total_employees AS detail_total_employees, lf.client_types,
    lf.comp_pct_aum, lf.comp_hourly, lf.comp_fixed, lf.comp_commissions, lf.comp_performance,
    lf.aum_discretionary, lf.aum_non_discretionary,
    lf.svc_financial_planning, lf.svc_portfolio_individuals, lf.svc_portfolio_pooled,
    lf.svc_portfolio_institutional, lf.svc_pension_consulting, lf.svc_adviser_selection,
    lf.wrap_fee_program, lf.financial_planning_clients,
    lf.biz_broker_dealer, lf.biz_insurance, lf.biz_real_estate, lf.biz_accountant,
    lf.aff_broker_dealer, lf.aff_bank, lf.aff_insurance,
    lf.sec_registered, lf.exempt_reporting, lf.state_registered,
    lf.custody_client_cash, lf.custody_client_securities,
    lf.has_any_drp, lf.drp_criminal_firm, lf.drp_regulatory_firm,
    bc.num_branch_offices, bc.num_registered_reps, bc.registration_status AS brokercheck_status,
    ex.cik, ex.match_type AS xref_match_type, ex.confidence AS xref_confidence,
    ee.sic, ee.sic_description, ee.tickers, ee.exchanges,
    be.investment_strategies, be.industry_specializations, be.min_account_size,
    be.fee_schedule, be.target_clients,
    ce.firm_type AS crs_firm_type, ce.key_services AS crs_key_services,
    ce.has_disciplinary_history AS crs_disciplinary_history
FROM fed_data.adv_firms af
LEFT JOIN LATERAL (
    SELECT * FROM fed_data.adv_filings
    WHERE crd_number = af.crd_number ORDER BY filing_date DESC LIMIT 1
) lf ON true
LEFT JOIN fed_data.brokercheck bc ON bc.crd_number = af.crd_number
LEFT JOIN fed_data.entity_xref ex ON ex.crd_number = af.crd_number AND ex.confidence >= 0.7
LEFT JOIN fed_data.edgar_entities ee ON ee.cik = ex.cik
LEFT JOIN LATERAL (
    SELECT investment_strategies, industry_specializations, min_account_size, fee_schedule, target_clients
    FROM fed_data.adv_brochure_enrichment WHERE crd_number = af.crd_number ORDER BY enriched_at DESC LIMIT 1
) be ON true
LEFT JOIN LATERAL (
    SELECT firm_type, key_services, has_disciplinary_history
    FROM fed_data.adv_crs_enrichment WHERE crd_number = af.crd_number ORDER BY enriched_at DESC LIMIT 1
) ce ON true
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_firm_combined_crd ON fed_data.mv_firm_combined (crd_number);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_state ON fed_data.mv_firm_combined (state);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_aum ON fed_data.mv_firm_combined (aum DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_drp ON fed_data.mv_firm_combined (has_any_drp) WHERE has_any_drp = true;
