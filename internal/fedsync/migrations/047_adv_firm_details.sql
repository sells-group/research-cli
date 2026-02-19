-- 047: ADV firm details from FOIA CSV Items 5-7
-- Stores structured checkbox/flag data from Form ADV Items 5D-7A.

CREATE TABLE IF NOT EXISTS fed_data.adv_firm_details (
    crd_number              INTEGER PRIMARY KEY,

    -- Item 1 additions
    legal_name              VARCHAR(300),
    form_of_org             VARCHAR(100),
    num_other_offices       INTEGER,

    -- Item 5A-C
    total_employees         INTEGER,
    num_adviser_reps        INTEGER,

    -- Item 5D: Client type breakdown (JSONB array)
    -- [{type, count, pct_raum, raum}, ...]
    client_types            JSONB,

    -- Item 5E: Compensation (Y/N → bool)
    comp_pct_aum            BOOLEAN DEFAULT false,
    comp_hourly             BOOLEAN DEFAULT false,
    comp_subscription       BOOLEAN DEFAULT false,
    comp_fixed              BOOLEAN DEFAULT false,
    comp_commissions        BOOLEAN DEFAULT false,
    comp_performance        BOOLEAN DEFAULT false,
    comp_other              BOOLEAN DEFAULT false,

    -- Item 5F: AUM breakdown
    aum_discretionary       BIGINT,
    aum_non_discretionary   BIGINT,
    aum_total               BIGINT,

    -- Item 5G: Advisory services (Y/N → bool)
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

    -- Item 5I: Wrap fee
    wrap_fee_program        BOOLEAN DEFAULT false,
    wrap_fee_raum           BIGINT,

    -- Item 5J-K
    financial_planning_clients INTEGER,

    -- Item 6A: Other business activities (Y/N → bool)
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

    -- Item 7A: Financial affiliations (Y/N → bool)
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

    updated_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_firm_details_comp
    ON fed_data.adv_firm_details (comp_pct_aum, comp_performance);
CREATE INDEX IF NOT EXISTS idx_firm_details_svc
    ON fed_data.adv_firm_details (svc_financial_planning, svc_portfolio_individuals);
