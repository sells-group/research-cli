-- 088: Universal company-to-MSA view for all companies (not just ADV firms).
-- v_firm_msa (from 085) only covers ADV firms linked via company_matches.
-- This view works for any company with a geocoded address.

CREATE OR REPLACE VIEW public.v_company_msa AS
SELECT DISTINCT ON (c.id)
    c.id              AS company_id,
    c.domain,
    a.id              AS address_id,
    a.latitude,
    a.longitude,
    a.geocode_quality,
    a.county_fips,
    am.cbsa_code,
    cb.name           AS msa_name,
    am.is_within,
    am.distance_km,
    am.centroid_km,
    am.edge_km,
    am.classification
FROM public.companies c
JOIN public.company_addresses a ON a.company_id = c.id
LEFT JOIN public.address_msa am ON am.address_id = a.id
LEFT JOIN public.cbsa_areas cb ON cb.cbsa_code = am.cbsa_code
WHERE a.latitude IS NOT NULL
ORDER BY c.id, a.is_primary DESC, am.centroid_km ASC NULLS LAST;
