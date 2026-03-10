-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_infrastructure_type
    ON geo.infrastructure (type);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_infrastructure_source_type
    ON geo.infrastructure (source, type);

-- +goose Down
DROP INDEX IF EXISTS geo.idx_infrastructure_type;
DROP INDEX IF EXISTS geo.idx_infrastructure_source_type;
