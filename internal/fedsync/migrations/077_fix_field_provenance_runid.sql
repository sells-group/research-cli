-- Fix field_provenance: run_id was BIGINT but runs.id is TEXT.
-- Safe to drop+recreate because the table has never been populated.
DROP TABLE IF EXISTS public.field_provenance;

CREATE TABLE public.field_provenance (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               TEXT REFERENCES runs(id),
    company_url          VARCHAR(500) NOT NULL,
    field_key            VARCHAR(100) NOT NULL,
    winner_source        VARCHAR(50),
    winner_value         TEXT,
    raw_confidence       NUMERIC(4,3),
    effective_confidence NUMERIC(4,3),
    data_as_of           TIMESTAMPTZ,
    threshold            NUMERIC(3,2),
    threshold_met        BOOLEAN NOT NULL DEFAULT FALSE,
    attempts             JSONB,
    premium_cost_usd     NUMERIC(8,4) DEFAULT 0,
    previous_value       TEXT,
    previous_run_id      TEXT,
    value_changed        BOOLEAN DEFAULT FALSE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_field_provenance_run ON public.field_provenance (run_id);
CREATE INDEX idx_field_provenance_company ON public.field_provenance (company_url, field_key);
