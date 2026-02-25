-- 084_geocode_cache.sql: geocode result cache for deduplication.
-- Stores PostGIS geocode() results keyed by SHA-256 of normalized address.

CREATE TABLE IF NOT EXISTS public.geocode_cache (
    address_hash VARCHAR(64) PRIMARY KEY,
    latitude     NUMERIC(9,6) NOT NULL,
    longitude    NUMERIC(9,6) NOT NULL,
    quality      VARCHAR(20) NOT NULL,
    rating       INTEGER,
    cached_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_geocode_cache_at ON public.geocode_cache (cached_at);

COMMENT ON TABLE public.geocode_cache IS 'Caches PostGIS geocode() results keyed by SHA-256 of normalized address';
