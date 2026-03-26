-- +goose Up
CREATE TABLE IF NOT EXISTS public.dead_letter_queue (
    id             TEXT PRIMARY KEY,
    company        JSONB NOT NULL,
    error          TEXT NOT NULL,
    error_type     TEXT NOT NULL DEFAULT 'transient',
    failed_phase   TEXT,
    retry_count    INTEGER NOT NULL DEFAULT 0,
    max_retries    INTEGER NOT NULL DEFAULT 3,
    next_retry_at  TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON public.dead_letter_queue (error_type);
CREATE INDEX IF NOT EXISTS idx_dlq_next_retry ON public.dead_letter_queue (next_retry_at);

-- +goose Down
DROP TABLE IF EXISTS public.dead_letter_queue;
