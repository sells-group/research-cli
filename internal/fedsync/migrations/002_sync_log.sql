-- 002_sync_log.sql: Sync log tracks when each dataset was last synced.
CREATE TABLE IF NOT EXISTS fed_data.sync_log (
    id          BIGSERIAL PRIMARY KEY,
    dataset     TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'running',  -- running, complete, failed
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    rows_synced BIGINT      DEFAULT 0,
    error       TEXT,
    metadata    JSONB
);

CREATE INDEX IF NOT EXISTS idx_sync_log_dataset ON fed_data.sync_log (dataset);
CREATE INDEX IF NOT EXISTS idx_sync_log_status ON fed_data.sync_log (status);
CREATE INDEX IF NOT EXISTS idx_sync_log_started ON fed_data.sync_log (started_at DESC);
