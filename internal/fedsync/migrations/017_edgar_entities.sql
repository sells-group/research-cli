CREATE TABLE IF NOT EXISTS fed_data.edgar_entities (
    cik             VARCHAR(10) PRIMARY KEY,
    entity_name     VARCHAR(200) NOT NULL,
    entity_type     VARCHAR(20),
    sic             VARCHAR(4),
    sic_description VARCHAR(200),
    state_of_inc    VARCHAR(5),
    state_of_business VARCHAR(5),
    ein             VARCHAR(10),
    tickers         TEXT[],
    exchanges       TEXT[],
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_edgar_entities_name ON fed_data.edgar_entities USING gin (entity_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_edgar_entities_sic ON fed_data.edgar_entities (sic);
