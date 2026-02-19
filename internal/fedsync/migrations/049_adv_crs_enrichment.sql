-- 049: ADV CRS enrichment via Haiku extraction
-- Stores structured data extracted from adv_crs.text_content.

CREATE TABLE IF NOT EXISTS fed_data.adv_crs_enrichment (
    crd_number               INTEGER NOT NULL,
    crs_id                   VARCHAR(50) NOT NULL,
    firm_type                VARCHAR(100),
    key_services             TEXT,
    fee_types                JSONB,
    has_disciplinary_history BOOLEAN,
    conflicts_of_interest    TEXT,
    model                    VARCHAR(50),
    input_tokens             INTEGER,
    output_tokens            INTEGER,
    enriched_at              TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, crs_id)
);

CREATE INDEX IF NOT EXISTS idx_crs_enrich_firm_type
    ON fed_data.adv_crs_enrichment (firm_type);
