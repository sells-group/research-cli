-- Multi-value tags per company (services, industries, etc.).
CREATE TABLE IF NOT EXISTS public.company_tags (
    company_id      BIGINT NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    tag_type        VARCHAR(50) NOT NULL,
    tag_value       VARCHAR(200) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (company_id, tag_type, tag_value)
);

CREATE INDEX IF NOT EXISTS idx_company_tags_type ON public.company_tags (tag_type, tag_value);
