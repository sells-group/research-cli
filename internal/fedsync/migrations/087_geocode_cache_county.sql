ALTER TABLE public.geocode_cache ADD COLUMN IF NOT EXISTS county_fips VARCHAR(5);
