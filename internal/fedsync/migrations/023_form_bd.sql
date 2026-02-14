CREATE TABLE IF NOT EXISTS fed_data.form_bd (
    crd_number      INTEGER PRIMARY KEY,
    sec_number      VARCHAR(20),
    firm_name       VARCHAR(300),
    city            VARCHAR(100),
    state           CHAR(2),
    fiscal_year_end VARCHAR(4),
    num_reps        INTEGER,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
