-- Flexible key-value identifiers linking companies to external systems.
CREATE TABLE IF NOT EXISTS public.company_identifiers (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    system          VARCHAR(50) NOT NULL,
    identifier      VARCHAR(200) NOT NULL,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (company_id, system, identifier)
);

CREATE INDEX IF NOT EXISTS idx_company_identifiers_lookup ON public.company_identifiers (system, identifier);
