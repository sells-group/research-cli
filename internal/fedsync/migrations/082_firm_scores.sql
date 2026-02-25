-- 082_firm_scores.sql: scoring results from multi-pass firm evaluation funnel.
-- Stores ADV scores (pass 0), website scores (pass 1), T1 scores (pass 2), and deep enrichment scores (pass 3).

CREATE TABLE IF NOT EXISTS fed_data.firm_scores (
    crd_number       INTEGER      NOT NULL,
    scored_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    pass             SMALLINT     NOT NULL CHECK (pass BETWEEN 0 AND 3),
    score            NUMERIC(5,2) NOT NULL,
    component_scores JSONB,
    matched_keywords JSONB,
    passed           BOOLEAN      NOT NULL,
    config_hash      VARCHAR(64),
    PRIMARY KEY (crd_number, pass, scored_at)
);

COMMENT ON TABLE fed_data.firm_scores IS 'Multi-pass firm scoring results for M&A target identification funnel';
COMMENT ON COLUMN fed_data.firm_scores.pass IS '0=ADV, 1=Website, 2=T1 Extract, 3=Deep Enrichment';
COMMENT ON COLUMN fed_data.firm_scores.component_scores IS 'Individual scoring components (e.g., aum_fit, growth_score, client_quality)';
COMMENT ON COLUMN fed_data.firm_scores.matched_keywords IS 'Keywords that matched during scoring (geo, industry, succession)';
COMMENT ON COLUMN fed_data.firm_scores.config_hash IS 'SHA-256 of scoring config for reproducibility tracking';

CREATE INDEX IF NOT EXISTS idx_firm_scores_pass_score
    ON fed_data.firm_scores (pass, score DESC);

CREATE INDEX IF NOT EXISTS idx_firm_scores_crd
    ON fed_data.firm_scores (crd_number);

CREATE INDEX IF NOT EXISTS idx_firm_scores_latest
    ON fed_data.firm_scores (crd_number, pass, scored_at DESC);
