-- +goose Up
-- Spatial index on company_addresses for MVT tile generation.
CREATE INDEX IF NOT EXISTS idx_company_addresses_geom ON public.company_addresses USING GIST (geom);

-- View for company MVT tiles: joins addresses with company metadata.
CREATE OR REPLACE VIEW geo.company_points AS
SELECT
    a.id,
    a.geom,
    c.name,
    c.domain,
    c.city,
    c.state,
    c.enrichment_score AS score,
    c.id AS company_id
FROM public.company_addresses a
JOIN public.companies c ON c.id = a.company_id
WHERE a.is_primary = true AND a.geom IS NOT NULL;

-- +goose Down
DROP VIEW IF EXISTS geo.company_points;
DROP INDEX IF EXISTS public.idx_company_addresses_geom;
