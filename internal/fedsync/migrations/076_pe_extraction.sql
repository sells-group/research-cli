-- PE Firm Extraction Pipeline
-- Tables for storing identified PE firms, their RIA ownership links,
-- crawled website content, extraction runs, and extracted answers.

-- pe_firms: Master table for identified PE/aggregator firms
CREATE TABLE IF NOT EXISTS fed_data.pe_firms (
    pe_firm_id   BIGSERIAL PRIMARY KEY,
    firm_name    VARCHAR(300) NOT NULL,
    firm_type    VARCHAR(100),       -- PE, aggregator, holding_company, family_office, other
    website_url  VARCHAR(500),
    website_source VARCHAR(50),      -- adv_firms, edgar, perplexity, manual
    hq_city      VARCHAR(200),
    hq_state     VARCHAR(10),
    hq_address   TEXT,
    year_founded  INT,
    identified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_pe_firms_name UNIQUE (firm_name)
);

CREATE INDEX IF NOT EXISTS idx_pe_firms_firm_type ON fed_data.pe_firms (firm_type);
CREATE INDEX IF NOT EXISTS idx_pe_firms_trgm ON fed_data.pe_firms USING gin (firm_name gin_trgm_ops);

-- pe_firm_rias: PE firm ↔ RIA ownership links
CREATE TABLE IF NOT EXISTS fed_data.pe_firm_rias (
    pe_firm_id   BIGINT NOT NULL REFERENCES fed_data.pe_firms(pe_firm_id),
    crd_number   INT NOT NULL,
    ownership_pct NUMERIC(5,2),
    is_control   BOOLEAN DEFAULT false,
    owner_type   VARCHAR(100),
    linked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pe_firm_id, crd_number)
);

CREATE INDEX IF NOT EXISTS idx_pe_firm_rias_crd ON fed_data.pe_firm_rias (crd_number);

-- pe_crawl_cache: Cached crawl pages per PE firm
CREATE TABLE IF NOT EXISTS fed_data.pe_crawl_cache (
    pe_firm_id   BIGINT NOT NULL REFERENCES fed_data.pe_firms(pe_firm_id),
    url          VARCHAR(2000) NOT NULL,
    page_type    VARCHAR(50),        -- homepage, about, team, portfolio, strategy, news, contact, careers, other
    title        VARCHAR(500),
    markdown     TEXT,
    status_code  INT,
    crawled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pe_firm_id, url)
);

CREATE INDEX IF NOT EXISTS idx_pe_crawl_cache_type ON fed_data.pe_crawl_cache (pe_firm_id, page_type);

-- pe_extraction_runs: Run tracking (mirrors adv_extraction_runs)
CREATE TABLE IF NOT EXISTS fed_data.pe_extraction_runs (
    id              BIGSERIAL PRIMARY KEY,
    pe_firm_id      BIGINT NOT NULL REFERENCES fed_data.pe_firms(pe_firm_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'running',  -- running, complete, failed
    tier_completed  INT,
    total_questions INT,
    answered        INT,
    pages_crawled   INT,
    input_tokens    BIGINT,
    output_tokens   BIGINT,
    cost_usd        NUMERIC(10,4),
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pe_extraction_runs_firm ON fed_data.pe_extraction_runs (pe_firm_id, status);

-- pe_answers: One row per question per firm
CREATE TABLE IF NOT EXISTS fed_data.pe_answers (
    pe_firm_id      BIGINT NOT NULL REFERENCES fed_data.pe_firms(pe_firm_id),
    question_key    VARCHAR(100) NOT NULL,
    value           JSONB,
    confidence      NUMERIC(3,2),
    tier            INT,
    reasoning       TEXT,
    source_url      VARCHAR(2000),
    source_page_type VARCHAR(50),
    model           VARCHAR(100),
    input_tokens    INT,
    output_tokens   INT,
    run_id          BIGINT REFERENCES fed_data.pe_extraction_runs(id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pe_firm_id, question_key)
);

CREATE INDEX IF NOT EXISTS idx_pe_answers_value ON fed_data.pe_answers USING gin (value);
CREATE INDEX IF NOT EXISTS idx_pe_answers_run ON fed_data.pe_answers (run_id);

-- mv_pe_intelligence: Materialized view joining firms + answers + aggregated RIA data
CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_pe_intelligence AS
SELECT
    pf.pe_firm_id,
    pf.firm_name,
    pf.firm_type,
    pf.website_url,
    pf.hq_city,
    pf.hq_state,
    pf.year_founded,

    -- RIA ownership stats
    ria.ria_count,
    ria.total_ria_aum,
    ria.avg_ria_aum,
    ria.ria_states,

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

    -- Answer stats
    ans_stats.answer_count,
    ans_stats.avg_confidence,

    -- Latest run info
    lr.latest_run_at,
    lr.latest_run_status,
    lr.latest_cost_usd

FROM fed_data.pe_firms pf

LEFT JOIN LATERAL (
    SELECT
        count(*) AS ria_count,
        COALESCE(sum(fi.aum_total), 0) AS total_ria_aum,
        COALESCE(avg(fi.aum_total), 0) AS avg_ria_aum,
        array_agg(DISTINCT f.state) FILTER (WHERE f.state IS NOT NULL) AS ria_states
    FROM fed_data.pe_firm_rias pr
    JOIN fed_data.adv_firms f ON f.crd_number = pr.crd_number
    LEFT JOIN LATERAL (
        SELECT aum_total FROM fed_data.adv_filings fi2
        WHERE fi2.crd_number = pr.crd_number
        ORDER BY fi2.filing_date DESC LIMIT 1
    ) fi ON true
    WHERE pr.pe_firm_id = pf.pe_firm_id
) ria ON true

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

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pe_intelligence_pk ON fed_data.mv_pe_intelligence (pe_firm_id);
CREATE INDEX IF NOT EXISTS idx_mv_pe_intelligence_ria_count ON fed_data.mv_pe_intelligence (ria_count DESC);
CREATE INDEX IF NOT EXISTS idx_mv_pe_intelligence_aum ON fed_data.mv_pe_intelligence (total_ria_aum DESC);
