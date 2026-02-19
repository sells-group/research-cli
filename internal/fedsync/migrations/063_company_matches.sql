-- Bridge table linking companies to fed_data entities.
CREATE TABLE IF NOT EXISTS public.company_matches (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    matched_source  VARCHAR(50) NOT NULL,
    matched_key     VARCHAR(200) NOT NULL,
    match_type      VARCHAR(30) NOT NULL,
    confidence      NUMERIC(3,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (company_id, matched_source, matched_key)
);

CREATE INDEX IF NOT EXISTS idx_company_matches_source ON public.company_matches (matched_source, matched_key);
