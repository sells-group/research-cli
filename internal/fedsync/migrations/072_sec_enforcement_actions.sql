-- SEC enforcement actions for regulatory risk scoring.
CREATE TABLE IF NOT EXISTS fed_data.sec_enforcement_actions (
    action_id       VARCHAR(50) PRIMARY KEY,
    action_type     VARCHAR(50) NOT NULL,
    respondent_name VARCHAR(300),
    crd_number      INTEGER,
    cik             VARCHAR(20),
    action_date     DATE,
    description     TEXT,
    outcome         VARCHAR(100),
    penalty_amount  BIGINT,
    url             VARCHAR(500),
    synced_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enforcement_crd
    ON fed_data.sec_enforcement_actions (crd_number);
CREATE INDEX IF NOT EXISTS idx_enforcement_date
    ON fed_data.sec_enforcement_actions (action_date DESC);
CREATE INDEX IF NOT EXISTS idx_enforcement_respondent
    ON fed_data.sec_enforcement_actions (respondent_name);
