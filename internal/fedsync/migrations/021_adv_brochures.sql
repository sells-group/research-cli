CREATE TABLE IF NOT EXISTS fed_data.adv_brochures (
    crd_number      INTEGER NOT NULL,
    brochure_id     VARCHAR(50) NOT NULL,
    filing_date     DATE,
    text_content    TEXT,
    extracted_at    TIMESTAMPTZ,
    PRIMARY KEY (crd_number, brochure_id)
);
