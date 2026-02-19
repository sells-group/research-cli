-- Structured license/registration records per company.
CREATE TABLE IF NOT EXISTS public.licenses (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    license_type    VARCHAR(100) NOT NULL,
    license_number  VARCHAR(100),
    authority       VARCHAR(200),
    state           VARCHAR(10),
    status          VARCHAR(50),
    issued_date     DATE,
    expiry_date     DATE,
    source          VARCHAR(50),
    raw_text        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_licenses_company ON public.licenses (company_id);
CREATE INDEX IF NOT EXISTS idx_licenses_type_state ON public.licenses (license_type, state);
