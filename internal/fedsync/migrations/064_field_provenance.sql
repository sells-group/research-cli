-- Waterfall provenance: per-field per-run audit trail of source evaluation.
CREATE TABLE IF NOT EXISTS public.field_provenance (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT REFERENCES runs(id),
    company_url         VARCHAR(500) NOT NULL,
    field_key           VARCHAR(100) NOT NULL,
    winner_source       VARCHAR(50),
    winner_value        TEXT,
    raw_confidence      NUMERIC(4,3),
    effective_confidence NUMERIC(4,3),
    data_as_of          TIMESTAMPTZ,
    threshold           NUMERIC(3,2),
    threshold_met       BOOLEAN NOT NULL DEFAULT FALSE,
    attempts            JSONB,
    premium_cost_usd    NUMERIC(8,4) DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_field_provenance_run ON public.field_provenance (run_id);
CREATE INDEX IF NOT EXISTS idx_field_provenance_company ON public.field_provenance (company_url, field_key);

-- Premium provider query log.
CREATE TABLE IF NOT EXISTS public.premium_queries (
    id                BIGSERIAL PRIMARY KEY,
    company_url       VARCHAR(500) NOT NULL,
    provider          VARCHAR(50) NOT NULL,
    fields_requested  TEXT[],
    fields_returned   TEXT[],
    cost_usd          NUMERIC(8,4) NOT NULL DEFAULT 0,
    duration_ms       INTEGER,
    raw_response      JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_premium_queries_company ON public.premium_queries (company_url);
CREATE INDEX IF NOT EXISTS idx_premium_queries_provider ON public.premium_queries (provider);
