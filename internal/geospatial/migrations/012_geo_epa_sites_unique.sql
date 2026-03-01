CREATE UNIQUE INDEX IF NOT EXISTS idx_epa_sites_source_source_id
    ON geo.epa_sites (source, source_id);
