CREATE TABLE IF NOT EXISTS fed_data.brokercheck (
    crd_number      INTEGER PRIMARY KEY,
    firm_name       VARCHAR(300) NOT NULL,
    sec_number      VARCHAR(20),
    main_addr_city  VARCHAR(100),
    main_addr_state CHAR(2),
    num_branch_offices INTEGER,
    num_registered_reps INTEGER,
    registration_status VARCHAR(50),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_brokercheck_name ON fed_data.brokercheck USING gin (firm_name gin_trgm_ops);
