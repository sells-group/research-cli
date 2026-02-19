-- Time-series financial metrics per company.
CREATE TABLE IF NOT EXISTS public.company_financials (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    period_type     VARCHAR(20) NOT NULL,
    period_date     DATE NOT NULL,
    metric          VARCHAR(50) NOT NULL,
    value           NUMERIC(18,2),
    source          VARCHAR(50),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (company_id, period_type, period_date, metric, source)
);

CREATE INDEX IF NOT EXISTS idx_company_financials_company ON public.company_financials (company_id, metric);
CREATE INDEX IF NOT EXISTS idx_company_financials_period ON public.company_financials (period_date DESC);
