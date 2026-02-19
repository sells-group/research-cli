-- Multiple addresses per company with type and provenance.
CREATE TABLE IF NOT EXISTS public.company_addresses (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    address_type    VARCHAR(30) NOT NULL,
    street          VARCHAR(300),
    city            VARCHAR(100),
    state           VARCHAR(10),
    zip_code        VARCHAR(20),
    country         VARCHAR(50) DEFAULT 'US',
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    source          VARCHAR(50),
    confidence      NUMERIC(3,2),
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_company_addresses_company ON public.company_addresses (company_id);
CREATE INDEX IF NOT EXISTS idx_company_addresses_state ON public.company_addresses (state);
