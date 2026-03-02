-- 085_geocode_cache_v2.sql: geocode cache improvements + CRD→MSA linkage view.
-- Adds negative caching support and a view bridging ADV CRD → company → address → MSA.

-- Add matched column for negative caching (previously only matches were cached).
ALTER TABLE public.geocode_cache ADD COLUMN IF NOT EXISTS matched BOOLEAN NOT NULL DEFAULT true;

-- View: CRD → company → address → MSA (closest MSA per firm's primary address).
-- Used by the scorer to look up spatial data for ADV firms via company_matches.
CREATE OR REPLACE VIEW public.v_firm_msa AS
SELECT DISTINCT ON (cm.matched_key)
    cm.matched_key::integer AS crd_number,
    c.id AS company_id,
    a.id AS address_id,
    a.latitude,
    a.longitude,
    a.geocode_quality,
    am.cbsa_code,
    cb.name AS msa_name,
    am.is_within,
    am.distance_km,
    am.centroid_km,
    am.edge_km,
    am.classification
FROM public.company_matches cm
JOIN public.companies c ON c.id = cm.company_id
JOIN public.company_addresses a ON a.company_id = c.id
LEFT JOIN public.address_msa am ON am.address_id = a.id
LEFT JOIN public.cbsa_areas cb ON cb.cbsa_code = am.cbsa_code
WHERE cm.matched_source = 'adv_firms'
ORDER BY cm.matched_key, a.is_primary DESC, am.centroid_km ASC NULLS LAST;
