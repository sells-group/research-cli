CREATE TABLE IF NOT EXISTS fed_data.adv_crs (
    crd_number  INTEGER NOT NULL,
    crs_id      VARCHAR(50) NOT NULL,
    filing_date DATE,
    text_content TEXT,
    extracted_at TIMESTAMPTZ,
    PRIMARY KEY (crd_number, crs_id)
);
