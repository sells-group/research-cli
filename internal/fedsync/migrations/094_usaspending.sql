-- USAspending federal awards (grants, loans, direct payments, contracts)
CREATE TABLE IF NOT EXISTS fed_data.usaspending_awards (
    award_id                 TEXT        PRIMARY KEY,
    award_type               TEXT        NOT NULL,
    award_type_code          TEXT,
    piid                     TEXT,
    fain                     TEXT,
    uri                      TEXT,
    awarding_agency_code     TEXT,
    awarding_agency_name     TEXT,
    awarding_sub_agency_code TEXT,
    awarding_sub_agency_name TEXT,
    funding_agency_code      TEXT,
    funding_agency_name      TEXT,
    recipient_uei            VARCHAR(12),
    recipient_duns           VARCHAR(13),
    recipient_name           TEXT,
    recipient_parent_uei     VARCHAR(12),
    recipient_parent_name    TEXT,
    recipient_address_line_1 TEXT,
    recipient_city           TEXT,
    recipient_state          CHAR(2),
    recipient_zip            TEXT,
    recipient_country        TEXT,
    total_obligated_amount   NUMERIC(15,2),
    total_outlayed_amount    NUMERIC(15,2),
    naics_code               VARCHAR(6),
    naics_description        TEXT,
    psc_code                 VARCHAR(4),
    cfda_number              TEXT,
    cfda_title               TEXT,
    award_base_action_date   DATE,
    award_latest_action_date DATE,
    period_of_perf_start     DATE,
    period_of_perf_end       DATE,
    last_modified_date       DATE,
    pop_city                 TEXT,
    pop_state                CHAR(2),
    pop_zip                  TEXT,
    pop_country              TEXT,
    award_description        TEXT,
    usaspending_permalink    TEXT,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_usaspending_recipient_uei ON fed_data.usaspending_awards (recipient_uei);
CREATE INDEX IF NOT EXISTS idx_usaspending_recipient_duns ON fed_data.usaspending_awards (recipient_duns);
CREATE INDEX IF NOT EXISTS idx_usaspending_agency ON fed_data.usaspending_awards (awarding_agency_code);
CREATE INDEX IF NOT EXISTS idx_usaspending_naics ON fed_data.usaspending_awards (naics_code);
CREATE INDEX IF NOT EXISTS idx_usaspending_cfda ON fed_data.usaspending_awards (cfda_number);
CREATE INDEX IF NOT EXISTS idx_usaspending_action_date ON fed_data.usaspending_awards (award_latest_action_date DESC);
CREATE INDEX IF NOT EXISTS idx_usaspending_modified ON fed_data.usaspending_awards (last_modified_date DESC);
CREATE INDEX IF NOT EXISTS idx_usaspending_name_trgm ON fed_data.usaspending_awards USING gin (recipient_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_usaspending_state ON fed_data.usaspending_awards (recipient_state);
