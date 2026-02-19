-- 048: ADV brochure enrichment via Haiku extraction
-- Stores structured data extracted from adv_brochures.text_content.

CREATE TABLE IF NOT EXISTS fed_data.adv_brochure_enrichment (
    crd_number               INTEGER NOT NULL,
    brochure_id              VARCHAR(50) NOT NULL,
    investment_strategies    JSONB,
    industry_specializations JSONB,
    min_account_size         BIGINT,
    fee_schedule             TEXT,
    target_clients           TEXT,
    model                    VARCHAR(50),
    input_tokens             INTEGER,
    output_tokens            INTEGER,
    enriched_at              TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, brochure_id)
);

CREATE INDEX IF NOT EXISTS idx_brochure_enrich_strategies
    ON fed_data.adv_brochure_enrichment USING gin (investment_strategies);
CREATE INDEX IF NOT EXISTS idx_brochure_enrich_industries
    ON fed_data.adv_brochure_enrichment USING gin (industry_specializations);
