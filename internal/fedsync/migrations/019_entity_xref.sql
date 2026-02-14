CREATE TABLE IF NOT EXISTS fed_data.entity_xref (
    id              BIGSERIAL PRIMARY KEY,
    crd_number      INTEGER,
    cik             VARCHAR(10),
    entity_name     VARCHAR(200),
    match_type      VARCHAR(20) NOT NULL,
    confidence      NUMERIC(3,2),
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_xref_crd_cik ON fed_data.entity_xref (crd_number, cik) WHERE crd_number IS NOT NULL AND cik IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_xref_crd ON fed_data.entity_xref (crd_number);
CREATE INDEX IF NOT EXISTS idx_entity_xref_cik ON fed_data.entity_xref (cik);
