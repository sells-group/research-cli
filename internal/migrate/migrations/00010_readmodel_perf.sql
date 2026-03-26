-- +goose NO TRANSACTION
-- +goose Up
-- Read-model performance helpers for dashboard and explorer queries.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sync_log_dataset_started_desc
    ON fed_data.sync_log (dataset, started_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sync_log_dataset_status_started_desc
    ON fed_data.sync_log (dataset, status, started_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_run_phases_started_name
    ON public.run_phases (started_at DESC, name);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_domain_trgm
    ON public.companies USING GIN (domain public.gin_trgm_ops);

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_dataset_status_latest AS
SELECT DISTINCT ON (dataset)
    dataset,
    status,
    COALESCE(rows_synced, 0) AS rows_synced,
    started_at,
    metadata
FROM fed_data.sync_log
ORDER BY dataset, started_at DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_dataset_status_latest_dataset
    ON fed_data.mv_dataset_status_latest (dataset);

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_sync_daily_trends AS
SELECT
    date_trunc('day', started_at)::date AS sync_date,
    dataset,
    COALESCE(SUM(rows_synced), 0) AS rows_synced
FROM fed_data.sync_log
GROUP BY sync_date, dataset;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sync_daily_trends_date_dataset
    ON fed_data.mv_sync_daily_trends (sync_date, dataset);

-- +goose Down
DROP INDEX IF EXISTS fed_data.idx_mv_sync_daily_trends_date_dataset;
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_sync_daily_trends;

DROP INDEX IF EXISTS fed_data.idx_mv_dataset_status_latest_dataset;
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_dataset_status_latest;

DROP INDEX CONCURRENTLY IF EXISTS public.idx_companies_domain_trgm;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_run_phases_started_name;
DROP INDEX CONCURRENTLY IF EXISTS fed_data.idx_sync_log_dataset_status_started_desc;
DROP INDEX CONCURRENTLY IF EXISTS fed_data.idx_sync_log_dataset_started_desc;
