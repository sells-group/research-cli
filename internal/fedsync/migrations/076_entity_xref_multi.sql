-- Generalized entity cross-reference linking entities across all federal datasets.
-- Expands beyond the CRD-CIK xref (019) to support matching via name, geography,
-- NAICS, and other strategies across Census, BLS, SEC, FINRA, OSHA, EPA, and SBA data.
CREATE TABLE IF NOT EXISTS fed_data.entity_xref_multi (
    id              BIGSERIAL PRIMARY KEY,
    source_dataset  VARCHAR(30) NOT NULL,
    source_id       VARCHAR(50) NOT NULL,
    target_dataset  VARCHAR(30) NOT NULL,
    target_id       VARCHAR(50) NOT NULL,
    entity_name     VARCHAR(300),
    match_type      VARCHAR(30) NOT NULL,
    confidence      NUMERIC(3,2) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_xref_multi_pair
    ON fed_data.entity_xref_multi (source_dataset, source_id, target_dataset, target_id);
CREATE INDEX IF NOT EXISTS idx_xref_multi_source
    ON fed_data.entity_xref_multi (source_dataset, source_id);
CREATE INDEX IF NOT EXISTS idx_xref_multi_target
    ON fed_data.entity_xref_multi (target_dataset, target_id);
CREATE INDEX IF NOT EXISTS idx_xref_multi_confidence
    ON fed_data.entity_xref_multi (confidence DESC);
CREATE INDEX IF NOT EXISTS idx_xref_multi_match_type
    ON fed_data.entity_xref_multi (match_type);
