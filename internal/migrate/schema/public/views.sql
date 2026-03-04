-- schema/public/views.sql
-- Public schema views

CREATE VIEW "public"."v_company_msa" AS
SELECT DISTINCT ON (c.id) c.id AS company_id,
    c.domain,
    a.id AS address_id,
    a.latitude,
    a.longitude,
    a.geocode_quality,
    a.county_fips,
    am.cbsa_code,
    cb.name AS msa_name,
    am.is_within,
    am.distance_km,
    am.centroid_km,
    am.edge_km,
    am.classification
FROM (((companies c
    JOIN company_addresses a ON ((a.company_id = c.id)))
    LEFT JOIN address_msa am ON ((am.address_id = a.id)))
    LEFT JOIN cbsa_areas cb ON (((cb.cbsa_code)::text = (am.cbsa_code)::text)))
WHERE (a.latitude IS NOT NULL)
ORDER BY c.id, a.is_primary DESC, am.centroid_km;

CREATE VIEW "public"."v_firm_msa" AS
SELECT DISTINCT ON (cm.matched_key) (cm.matched_key)::integer AS crd_number,
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
FROM ((((company_matches cm
    JOIN companies c ON ((c.id = cm.company_id)))
    JOIN company_addresses a ON ((a.company_id = c.id)))
    LEFT JOIN address_msa am ON ((am.address_id = a.id)))
    LEFT JOIN cbsa_areas cb ON (((cb.cbsa_code)::text = (am.cbsa_code)::text)))
WHERE ((cm.matched_source)::text = 'adv_firms'::text)
ORDER BY cm.matched_key, a.is_primary DESC, am.centroid_km;
