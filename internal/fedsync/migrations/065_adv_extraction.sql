-- ADV Intelligence Database: extraction runs, advisor answers, fund answers, materialized view.

-- Extraction run tracking
CREATE TABLE IF NOT EXISTS fed_data.adv_extraction_runs (
    id              BIGSERIAL PRIMARY KEY,
    crd_number      INTEGER NOT NULL,
    scope           VARCHAR(20) NOT NULL DEFAULT 'advisor',  -- 'advisor' or 'fund'
    fund_id         VARCHAR(20),                              -- NULL for advisor-level
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',   -- pending/running/complete/failed
    tier_completed  SMALLINT NOT NULL DEFAULT 0,              -- 0/1/2/3
    total_questions INTEGER NOT NULL DEFAULT 0,
    answered        INTEGER NOT NULL DEFAULT 0,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    cost_usd        NUMERIC(8,4) NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_adv_extraction_runs_crd
    ON fed_data.adv_extraction_runs (crd_number);
CREATE INDEX IF NOT EXISTS idx_adv_extraction_runs_status
    ON fed_data.adv_extraction_runs (status);

-- Advisor-level answers (one row per question per advisor)
CREATE TABLE IF NOT EXISTS fed_data.adv_advisor_answers (
    crd_number      INTEGER NOT NULL,
    question_key    VARCHAR(80) NOT NULL,
    value           JSONB,
    confidence      NUMERIC(3,2),
    tier            SMALLINT NOT NULL,
    reasoning       TEXT,
    source_doc      VARCHAR(20),    -- 'part1', 'part2', 'part3', 'cross_doc'
    source_section  VARCHAR(50),    -- e.g., 'item_4', 'structured'
    model           VARCHAR(50),
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    run_id          BIGINT REFERENCES fed_data.adv_extraction_runs(id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (crd_number, question_key)
);

CREATE INDEX IF NOT EXISTS idx_adv_advisor_answers_value
    ON fed_data.adv_advisor_answers USING GIN (value);
CREATE INDEX IF NOT EXISTS idx_adv_advisor_answers_confidence
    ON fed_data.adv_advisor_answers (confidence DESC);
CREATE INDEX IF NOT EXISTS idx_adv_advisor_answers_run
    ON fed_data.adv_advisor_answers (run_id);

-- Fund-level answers (one row per question per fund)
CREATE TABLE IF NOT EXISTS fed_data.adv_fund_answers (
    crd_number      INTEGER NOT NULL,
    fund_id         VARCHAR(20) NOT NULL,
    question_key    VARCHAR(80) NOT NULL,
    value           JSONB,
    confidence      NUMERIC(3,2),
    tier            SMALLINT NOT NULL,
    reasoning       TEXT,
    source_doc      VARCHAR(20),
    source_section  VARCHAR(50),
    model           VARCHAR(50),
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    run_id          BIGINT REFERENCES fed_data.adv_extraction_runs(id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (crd_number, fund_id, question_key)
);

CREATE INDEX IF NOT EXISTS idx_adv_fund_answers_value
    ON fed_data.adv_fund_answers USING GIN (value);
CREATE INDEX IF NOT EXISTS idx_adv_fund_answers_confidence
    ON fed_data.adv_fund_answers (confidence DESC);
CREATE INDEX IF NOT EXISTS idx_adv_fund_answers_run
    ON fed_data.adv_fund_answers (run_id);

-- Materialized view for M&A screening
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_adv_intelligence AS
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
    -- Flatten key answers for screening
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'investment_philosophy') AS investment_philosophy,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'fee_schedule_complete') AS fee_schedule,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'target_client_profile') AS target_client_profile,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'succession_plan') AS succession_plan,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'revenue_estimate') AS revenue_estimate,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'growth_strategy') AS growth_strategy,
    (SELECT a.value FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'ownership_structure') AS ownership_structure,
    (SELECT a.confidence FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number AND a.question_key = 'revenue_estimate') AS revenue_confidence,
    (SELECT count(*) FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number) AS total_answers,
    (SELECT avg(a.confidence) FROM fed_data.adv_advisor_answers a WHERE a.crd_number = f.crd_number) AS avg_confidence
FROM fed_data.adv_firms f
LEFT JOIN LATERAL (
    SELECT * FROM fed_data.adv_filings fi2
    WHERE fi2.crd_number = f.crd_number
    ORDER BY fi2.filing_date DESC
    LIMIT 1
) fi ON true
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
