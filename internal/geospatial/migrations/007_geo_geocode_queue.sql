CREATE TABLE IF NOT EXISTS geo.geocode_queue (
    id           SERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    address      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending, processing, complete, failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    result       JSONB,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_table, source_id)
);

CREATE INDEX IF NOT EXISTS idx_geocode_queue_status ON geo.geocode_queue (status);
CREATE INDEX IF NOT EXISTS idx_geocode_queue_source ON geo.geocode_queue (source_table, source_id);
