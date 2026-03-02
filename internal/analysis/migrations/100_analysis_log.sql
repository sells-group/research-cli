-- Analysis run history tracking.
CREATE TABLE IF NOT EXISTS geo.analysis_log (
    id            SERIAL PRIMARY KEY,
    analyzer      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running',
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    rows_affected BIGINT DEFAULT 0,
    error         TEXT,
    metadata      JSONB
);

CREATE INDEX IF NOT EXISTS idx_analysis_log_analyzer ON geo.analysis_log (analyzer);
CREATE INDEX IF NOT EXISTS idx_analysis_log_status ON geo.analysis_log (status);
CREATE INDEX IF NOT EXISTS idx_analysis_log_started_at ON geo.analysis_log (started_at DESC);
