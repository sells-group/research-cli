-- People associated with companies.
CREATE TABLE IF NOT EXISTS public.contacts (
    id                BIGSERIAL PRIMARY KEY,
    company_id        BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    full_name         VARCHAR(200),
    title             VARCHAR(200),
    role_type         VARCHAR(30),
    email             VARCHAR(254),
    phone             VARCHAR(30),
    linkedin_url      VARCHAR(500),
    ownership_pct     NUMERIC(5,2),
    is_control_person BOOLEAN DEFAULT FALSE,
    is_primary        BOOLEAN DEFAULT FALSE,
    source            VARCHAR(50),
    confidence        NUMERIC(3,2),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_company_role ON public.contacts (company_id, role_type);
CREATE INDEX IF NOT EXISTS idx_contacts_name_trgm ON public.contacts USING gin (full_name gin_trgm_ops);
