-- Golden record: one row per company, domain is the canonical identifier.
CREATE TABLE IF NOT EXISTS public.companies (
    id              BIGSERIAL PRIMARY KEY,

    -- Identity
    name            VARCHAR(300) NOT NULL,
    legal_name      VARCHAR(300),
    domain          VARCHAR(255) NOT NULL,
    website         VARCHAR(500),
    description     TEXT,
    naics_code      VARCHAR(6),
    sic_code        VARCHAR(4),
    business_model  VARCHAR(100),
    year_founded    SMALLINT,
    ownership_type  VARCHAR(50),
    phone           VARCHAR(30),
    email           VARCHAR(254),

    -- Size indicators
    employee_count  INTEGER,
    employee_estimate INTEGER,
    revenue_estimate BIGINT,
    revenue_range   VARCHAR(50),
    revenue_confidence NUMERIC(3,2),

    -- Denormalized primary address
    street          VARCHAR(300),
    city            VARCHAR(100),
    state           VARCHAR(10),
    zip_code        VARCHAR(20),
    country         VARCHAR(50) DEFAULT 'US',

    -- Enrichment metadata
    enrichment_score NUMERIC(4,2),
    last_enriched_at TIMESTAMPTZ,
    last_run_id      TEXT REFERENCES runs(id),

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_domain ON public.companies (domain);
CREATE INDEX IF NOT EXISTS idx_companies_name_trgm ON public.companies USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_state ON public.companies (state);
CREATE INDEX IF NOT EXISTS idx_companies_naics ON public.companies (naics_code);
