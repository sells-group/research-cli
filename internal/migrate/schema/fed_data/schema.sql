-- schema/fed_data/schema.sql
-- Federal data schema and sync log

-- Add new schema named "fed_data"
CREATE SCHEMA "fed_data";
-- Create "sync_log" table
CREATE TABLE "fed_data"."sync_log" (
  "id" bigserial NOT NULL,
  "dataset" text NOT NULL,
  "status" text NOT NULL DEFAULT 'running',
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "completed_at" timestamptz NULL,
  "rows_synced" bigint NULL DEFAULT 0,
  "error" text NULL,
  "metadata" jsonb NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_sync_log_dataset" to table: "sync_log"
CREATE INDEX "idx_sync_log_dataset" ON "fed_data"."sync_log" ("dataset");
-- Create index "idx_sync_log_started" to table: "sync_log"
CREATE INDEX "idx_sync_log_started" ON "fed_data"."sync_log" ("started_at" DESC);
-- Create index "idx_sync_log_status" to table: "sync_log"
CREATE INDEX "idx_sync_log_status" ON "fed_data"."sync_log" ("status");
