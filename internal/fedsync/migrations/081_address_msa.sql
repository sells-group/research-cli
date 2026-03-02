CREATE TABLE public.address_msa (
    id              BIGSERIAL PRIMARY KEY,
    address_id      BIGINT NOT NULL REFERENCES public.company_addresses(id) ON DELETE CASCADE,
    cbsa_code       VARCHAR(5) NOT NULL REFERENCES public.cbsa_areas(cbsa_code),
    is_within       BOOLEAN NOT NULL,
    distance_km     NUMERIC(8,2) NOT NULL,          -- 0 if within; edge distance if outside
    centroid_km     NUMERIC(8,2) NOT NULL,          -- distance from MSA centroid
    edge_km         NUMERIC(8,2) NOT NULL,          -- distance from nearest MSA boundary point
    classification  VARCHAR(20) NOT NULL,            -- 'urban_core', 'suburban', 'exurban', 'rural'
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (address_id, cbsa_code)
);

CREATE INDEX idx_addr_msa_address ON public.address_msa (address_id);
CREATE INDEX idx_addr_msa_cbsa ON public.address_msa (cbsa_code);
CREATE INDEX idx_addr_msa_class ON public.address_msa (classification);

-- Convenience view: company -> MSA with all distances
CREATE OR REPLACE VIEW public.v_company_msa AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.domain,
    a.id AS address_id,
    a.address_type,
    a.street, a.city, a.state, a.zip_code,
    a.latitude, a.longitude,
    cb.cbsa_code,
    cb.name AS msa_name,
    cb.lsad,
    am.is_within,
    am.distance_km,
    am.centroid_km,
    am.edge_km,
    am.classification
FROM public.companies c
JOIN public.company_addresses a ON a.company_id = c.id
JOIN public.address_msa am ON am.address_id = a.id
JOIN public.cbsa_areas cb ON cb.cbsa_code = am.cbsa_code
ORDER BY c.id, a.is_primary DESC, am.centroid_km ASC;
