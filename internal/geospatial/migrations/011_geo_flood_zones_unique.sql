CREATE UNIQUE INDEX IF NOT EXISTS idx_flood_zones_source_source_id
    ON geo.flood_zones (source, source_id);
