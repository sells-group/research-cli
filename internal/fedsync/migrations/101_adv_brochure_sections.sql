CREATE TABLE IF NOT EXISTS fed_data.adv_brochure_sections (
    crd_number    INTEGER NOT NULL,
    brochure_id   TEXT NOT NULL,
    section_key   TEXT NOT NULL,
    section_title TEXT,
    text_content  TEXT,
    tables        JSONB,
    metadata      JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (crd_number, brochure_id, section_key)
);
CREATE INDEX IF NOT EXISTS idx_brochure_sections_crd
    ON fed_data.adv_brochure_sections (crd_number);
