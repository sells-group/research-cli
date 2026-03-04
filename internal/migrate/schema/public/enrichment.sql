-- schema/public/enrichment.sql
-- Enrichment pipeline tables (runs, caches, provenance)

-- Add new schema named "public"
CREATE SCHEMA IF NOT EXISTS "public";
-- Set comment to schema: "public"
COMMENT ON SCHEMA "public" IS 'standard public schema';
-- Create "runs" table
CREATE TABLE "public"."runs" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "company" jsonb NOT NULL,
  "status" text NOT NULL DEFAULT 'queued',
  "result" jsonb NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_runs_status" to table: "runs"
CREATE INDEX "idx_runs_status" ON "public"."runs" ("status");
-- Create "run_phases" table
CREATE TABLE "public"."run_phases" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "run_id" text NOT NULL,
  "name" text NOT NULL,
  "status" text NOT NULL DEFAULT 'running',
  "result" jsonb NULL,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "run_phases_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_run_phases_run_id" to table: "run_phases"
CREATE INDEX "idx_run_phases_run_id" ON "public"."run_phases" ("run_id");
-- Create "crawl_cache" table
CREATE TABLE "public"."crawl_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "company_url" text NOT NULL,
  "pages" jsonb NOT NULL,
  "crawled_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "crawl_cache_company_url_key" UNIQUE ("company_url")
);
-- Create index "idx_crawl_cache_company_url" to table: "crawl_cache"
CREATE INDEX "idx_crawl_cache_company_url" ON "public"."crawl_cache" ("company_url");
-- Create index "idx_crawl_cache_expires_at" to table: "crawl_cache"
CREATE INDEX "idx_crawl_cache_expires_at" ON "public"."crawl_cache" ("expires_at");
-- Create index "idx_crawl_cache_url_expires" to table: "crawl_cache"
CREATE INDEX "idx_crawl_cache_url_expires" ON "public"."crawl_cache" ("company_url", "expires_at" DESC);
-- Create "linkedin_cache" table
CREATE TABLE "public"."linkedin_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "domain" text NOT NULL,
  "data" jsonb NOT NULL,
  "cached_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "linkedin_cache_domain_key" UNIQUE ("domain")
);
-- Create index "idx_linkedin_cache_domain" to table: "linkedin_cache"
CREATE INDEX "idx_linkedin_cache_domain" ON "public"."linkedin_cache" ("domain");
-- Create index "idx_linkedin_cache_expires_at" to table: "linkedin_cache"
CREATE INDEX "idx_linkedin_cache_expires_at" ON "public"."linkedin_cache" ("expires_at");
-- Create "scrape_cache" table
CREATE TABLE "public"."scrape_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "url_hash" text NOT NULL,
  "content" jsonb NOT NULL,
  "cached_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "scrape_cache_url_hash_key" UNIQUE ("url_hash")
);
-- Create index "idx_scrape_cache_expires_at" to table: "scrape_cache"
CREATE INDEX "idx_scrape_cache_expires_at" ON "public"."scrape_cache" ("expires_at");
-- Create index "idx_scrape_cache_url_hash" to table: "scrape_cache"
CREATE INDEX "idx_scrape_cache_url_hash" ON "public"."scrape_cache" ("url_hash");
-- Create "checkpoints" table
CREATE TABLE "public"."checkpoints" (
  "company_id" text NOT NULL,
  "phase" text NOT NULL,
  "data" jsonb NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("company_id")
);
-- Create "field_provenance" table
CREATE TABLE "public"."field_provenance" (
  "id" bigserial NOT NULL,
  "run_id" text NULL,
  "company_url" character varying(500) NOT NULL,
  "field_key" character varying(100) NOT NULL,
  "winner_source" character varying(50) NULL,
  "winner_value" text NULL,
  "raw_confidence" numeric(4,3) NULL,
  "effective_confidence" numeric(4,3) NULL,
  "data_as_of" timestamptz NULL,
  "threshold" numeric(3,2) NULL,
  "threshold_met" boolean NOT NULL DEFAULT false,
  "attempts" jsonb NULL,
  "premium_cost_usd" numeric(8,4) NULL DEFAULT 0,
  "previous_value" text NULL,
  "previous_run_id" text NULL,
  "value_changed" boolean NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "field_provenance_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_field_provenance_company" to table: "field_provenance"
CREATE INDEX "idx_field_provenance_company" ON "public"."field_provenance" ("company_url", "field_key");
-- Create index "idx_field_provenance_run" to table: "field_provenance"
CREATE INDEX "idx_field_provenance_run" ON "public"."field_provenance" ("run_id");
-- Create "premium_queries" table
CREATE TABLE "public"."premium_queries" (
  "id" bigserial NOT NULL,
  "company_url" character varying(500) NOT NULL,
  "provider" character varying(50) NOT NULL,
  "fields_requested" text[] NULL,
  "fields_returned" text[] NULL,
  "cost_usd" numeric(8,4) NOT NULL DEFAULT 0,
  "duration_ms" integer NULL,
  "raw_response" jsonb NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_premium_queries_company" to table: "premium_queries"
CREATE INDEX "idx_premium_queries_company" ON "public"."premium_queries" ("company_url");
-- Create index "idx_premium_queries_provider" to table: "premium_queries"
CREATE INDEX "idx_premium_queries_provider" ON "public"."premium_queries" ("provider");
