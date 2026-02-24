-- Extended BrokerCheck columns for disclosure tracking.
ALTER TABLE fed_data.brokercheck
    ADD COLUMN IF NOT EXISTS has_disclosures BOOLEAN,
    ADD COLUMN IF NOT EXISTS disclosure_count INTEGER,
    ADD COLUMN IF NOT EXISTS registration_status VARCHAR(50);
