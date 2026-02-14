CREATE TABLE IF NOT EXISTS fed_data.edgar_filings (
    accession_number VARCHAR(25) PRIMARY KEY,
    cik              VARCHAR(10) NOT NULL,
    form_type        VARCHAR(20) NOT NULL,
    filing_date      DATE NOT NULL,
    primary_doc      VARCHAR(200),
    primary_doc_desc VARCHAR(300),
    items            TEXT,
    size             INTEGER,
    is_xbrl          BOOLEAN DEFAULT false,
    is_inline_xbrl   BOOLEAN DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_cik ON fed_data.edgar_filings (cik);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_form ON fed_data.edgar_filings (form_type);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_date ON fed_data.edgar_filings (filing_date);
