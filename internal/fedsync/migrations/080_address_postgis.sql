-- Add geometry column to existing company_addresses table
ALTER TABLE public.company_addresses
    ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326),
    ADD COLUMN IF NOT EXISTS geocode_source VARCHAR(20),      -- 'census', 'google', 'manual'
    ADD COLUMN IF NOT EXISTS geocode_quality VARCHAR(20),     -- 'rooftop', 'range', 'centroid', 'approximate'
    ADD COLUMN IF NOT EXISTS geocoded_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_company_addresses_geom
    ON public.company_addresses USING GIST (geom)
    WHERE geom IS NOT NULL;

-- Trigger: auto-compute geom from lat/lon on INSERT or UPDATE
CREATE OR REPLACE FUNCTION trg_address_geom()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    ELSE
        NEW.geom := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS address_geom_sync ON public.company_addresses;
CREATE TRIGGER address_geom_sync
    BEFORE INSERT OR UPDATE OF latitude, longitude ON public.company_addresses
    FOR EACH ROW EXECUTE FUNCTION trg_address_geom();

-- Backfill geom for any rows that already have lat/lon (currently none, but safe)
UPDATE public.company_addresses
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND geom IS NULL;
