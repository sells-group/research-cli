CREATE TABLE IF NOT EXISTS fed_data.adv_owners (
    crd_number      INTEGER     NOT NULL,
    owner_name      VARCHAR(200) NOT NULL,
    owner_type      VARCHAR(50),
    ownership_pct   NUMERIC(5,2),
    is_control      BOOLEAN DEFAULT false,
    PRIMARY KEY (crd_number, owner_name)
);
CREATE INDEX IF NOT EXISTS idx_adv_owners_name ON fed_data.adv_owners USING gin (owner_name gin_trgm_ops);
