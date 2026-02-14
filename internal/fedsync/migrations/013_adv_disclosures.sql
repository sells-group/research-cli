CREATE TABLE IF NOT EXISTS fed_data.adv_disclosures (
    crd_number      INTEGER     NOT NULL,
    disclosure_type VARCHAR(100) NOT NULL,
    event_date      DATE,
    description     TEXT,
    id              BIGSERIAL PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS idx_adv_disclosures_crd ON fed_data.adv_disclosures (crd_number);
