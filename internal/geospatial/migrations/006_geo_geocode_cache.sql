CREATE TABLE IF NOT EXISTS geo.geocode_cache (
    address_hash TEXT PRIMARY KEY,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    quality      TEXT,
    rating       INTEGER,
    matched      BOOLEAN NOT NULL DEFAULT false,
    county_fips  TEXT,
    source       TEXT NOT NULL DEFAULT 'cascade',
    cached_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_geo_geocode_cache_cached_at ON geo.geocode_cache (cached_at);
