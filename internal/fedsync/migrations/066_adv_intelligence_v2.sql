-- ADV Intelligence v2: answer history, document sections, fund filings, computed metrics, rebuilt MV.

-- 1. Answer history (audit trail for re-extractions)
CREATE TABLE IF NOT EXISTS fed_data.adv_answer_history (
    id              BIGSERIAL PRIMARY KEY,
    crd_number      INTEGER NOT NULL,
    fund_id         VARCHAR(20),
    question_key    VARCHAR(80) NOT NULL,
    value           JSONB,
    confidence      NUMERIC(3,2),
    tier            SMALLINT,
    reasoning       TEXT,
    source_doc      VARCHAR(20),
    source_section  VARCHAR(50),
    model           VARCHAR(50),
    run_id          BIGINT,
    superseded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_by   BIGINT
);
CREATE INDEX IF NOT EXISTS idx_answer_hist_crd ON fed_data.adv_answer_history (crd_number, question_key);

-- 2. Document section index (tracks what sections are available per advisor)
CREATE TABLE IF NOT EXISTS fed_data.adv_document_sections (
    crd_number      INTEGER NOT NULL,
    doc_type        VARCHAR(10) NOT NULL,
    doc_id          VARCHAR(50) NOT NULL,
    section_key     VARCHAR(20) NOT NULL,
    section_title   VARCHAR(200),
    char_length     INTEGER,
    token_estimate  INTEGER,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (crd_number, doc_type, doc_id, section_key)
);

-- 3. Fund filing history (track fund AUM over time)
CREATE TABLE IF NOT EXISTS fed_data.adv_fund_filings (
    crd_number        INTEGER NOT NULL,
    fund_id           VARCHAR(50) NOT NULL,
    filing_date       DATE NOT NULL,
    gross_asset_value BIGINT,
    net_asset_value   BIGINT,
    fund_type         VARCHAR(100),
    PRIMARY KEY (crd_number, fund_id, filing_date)
);

-- 4. Computed metrics (Go-derived, per advisor)
CREATE TABLE IF NOT EXISTS fed_data.adv_computed_metrics (
    crd_number                 INTEGER PRIMARY KEY,
    revenue_estimate           BIGINT,
    blended_fee_rate_bps       INTEGER,
    revenue_per_client         INTEGER,
    aum_growth_cagr_pct        NUMERIC(5,2),
    client_growth_rate_pct     NUMERIC(5,2),
    employee_growth_rate_pct   NUMERIC(5,2),
    hnw_revenue_pct            NUMERIC(5,2),
    institutional_revenue_pct  NUMERIC(5,2),
    fund_aum_pct_total         NUMERIC(5,2),
    compensation_diversity     SMALLINT,
    business_complexity        SMALLINT,
    drp_severity               SMALLINT,
    acquisition_readiness      SMALLINT,
    computed_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 5. Rebuild materialized view with expanded answer keys + computed metrics
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
    -- Computed metrics
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
