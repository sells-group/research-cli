-- Add unique constraint on (source, source_id) to support ON CONFLICT upserts.
CREATE UNIQUE INDEX IF NOT EXISTS idx_infrastructure_source_source_id
    ON geo.infrastructure (source, source_id);
