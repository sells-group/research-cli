-- Raw source layer: full source data per company per provider.
CREATE TABLE IF NOT EXISTS public.company_sources (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    source          VARCHAR(50) NOT NULL,
    source_id       VARCHAR(200),
    raw_data        JSONB,
    extracted_fields JSONB,
    data_as_of      TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT REFERENCES runs(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (company_id, source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_company_sources_company ON public.company_sources (company_id);
CREATE INDEX IF NOT EXISTS idx_company_sources_raw ON public.company_sources USING gin (raw_data);
