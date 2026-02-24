-- Add filing_type column for amendment tracking.
ALTER TABLE fed_data.adv_filings
    ADD COLUMN IF NOT EXISTS filing_type VARCHAR(20);

COMMENT ON COLUMN fed_data.adv_filings.filing_type IS 'Filing type: annual, amendment, initial, etc.';
