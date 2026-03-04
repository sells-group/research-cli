-- Intelligence and scoring tables

-- Create "pe_firms" table
CREATE TABLE "fed_data"."pe_firms" (
  "pe_firm_id" bigserial NOT NULL,
  "firm_name" character varying(300) NOT NULL,
  "firm_type" character varying(100) NULL,
  "website_url" character varying(500) NULL,
  "website_source" character varying(50) NULL,
  "hq_city" character varying(200) NULL,
  "hq_state" character varying(10) NULL,
  "hq_address" text NULL,
  "year_founded" integer NULL,
  "identified_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  "linkedin_url" character varying(500) NULL,
  "twitter_url" character varying(500) NULL,
  "facebook_url" character varying(500) NULL,
  "instagram_url" character varying(500) NULL,
  "youtube_url" character varying(500) NULL,
  "crunchbase_url" character varying(500) NULL,
  PRIMARY KEY ("pe_firm_id"),
  CONSTRAINT "uq_pe_firms_name" UNIQUE ("firm_name")
);
-- Create index "idx_pe_firms_firm_type" to table: "pe_firms"
CREATE INDEX "idx_pe_firms_firm_type" ON "fed_data"."pe_firms" ("firm_type");
-- Create index "idx_pe_firms_trgm" to table: "pe_firms"
CREATE INDEX "idx_pe_firms_trgm" ON "fed_data"."pe_firms" USING GIN ("firm_name" public.gin_trgm_ops);

-- Create "pe_extraction_runs" table
CREATE TABLE "fed_data"."pe_extraction_runs" (
  "id" bigserial NOT NULL,
  "pe_firm_id" bigint NOT NULL,
  "status" character varying(20) NOT NULL DEFAULT 'running',
  "tier_completed" integer NULL,
  "total_questions" integer NULL,
  "answered" integer NULL,
  "pages_crawled" integer NULL,
  "input_tokens" bigint NULL,
  "output_tokens" bigint NULL,
  "cost_usd" numeric(10,4) NULL,
  "error_message" text NULL,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "completed_at" timestamptz NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "pe_extraction_runs_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_pe_extraction_runs_firm" to table: "pe_extraction_runs"
CREATE INDEX "idx_pe_extraction_runs_firm" ON "fed_data"."pe_extraction_runs" ("pe_firm_id", "status");

-- Create "pe_answers" table
CREATE TABLE "fed_data"."pe_answers" (
  "pe_firm_id" bigint NOT NULL,
  "question_key" character varying(100) NOT NULL,
  "value" jsonb NULL,
  "confidence" numeric(3,2) NULL,
  "tier" integer NULL,
  "reasoning" text NULL,
  "source_url" character varying(2000) NULL,
  "source_page_type" character varying(50) NULL,
  "model" character varying(100) NULL,
  "input_tokens" integer NULL,
  "output_tokens" integer NULL,
  "run_id" bigint NULL,
  "extracted_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("pe_firm_id", "question_key"),
  CONSTRAINT "pe_answers_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "pe_answers_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "fed_data"."pe_extraction_runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_pe_answers_run" to table: "pe_answers"
CREATE INDEX "idx_pe_answers_run" ON "fed_data"."pe_answers" ("run_id");
-- Create index "idx_pe_answers_value" to table: "pe_answers"
CREATE INDEX "idx_pe_answers_value" ON "fed_data"."pe_answers" USING GIN ("value");

-- Create "pe_crawl_cache" table
CREATE TABLE "fed_data"."pe_crawl_cache" (
  "pe_firm_id" bigint NOT NULL,
  "url" character varying(2000) NOT NULL,
  "page_type" character varying(50) NULL,
  "title" character varying(500) NULL,
  "markdown" text NULL,
  "status_code" integer NULL,
  "crawled_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("pe_firm_id", "url"),
  CONSTRAINT "pe_crawl_cache_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_pe_crawl_cache_type" to table: "pe_crawl_cache"
CREATE INDEX "idx_pe_crawl_cache_type" ON "fed_data"."pe_crawl_cache" ("pe_firm_id", "page_type");

-- Create "pe_firm_overrides" table
CREATE TABLE "fed_data"."pe_firm_overrides" (
  "pe_firm_id" bigint NOT NULL,
  "website_url_override" character varying(500) NOT NULL,
  "notes" text NULL,
  "created_by" character varying(100) NOT NULL DEFAULT 'manual',
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("pe_firm_id"),
  CONSTRAINT "pe_firm_overrides_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);

-- Create "pe_firm_rias" table
CREATE TABLE "fed_data"."pe_firm_rias" (
  "pe_firm_id" bigint NOT NULL,
  "crd_number" integer NOT NULL,
  "ownership_pct" numeric(5,2) NULL,
  "is_control" boolean NULL DEFAULT false,
  "owner_type" character varying(100) NULL,
  "linked_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("pe_firm_id", "crd_number"),
  CONSTRAINT "pe_firm_rias_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_pe_firm_rias_crd" to table: "pe_firm_rias"
CREATE INDEX "idx_pe_firm_rias_crd" ON "fed_data"."pe_firm_rias" ("crd_number");

-- Create "firm_scores" table
CREATE TABLE "fed_data"."firm_scores" (
  "crd_number" integer NOT NULL,
  "scored_at" timestamptz NOT NULL DEFAULT now(),
  "pass" smallint NOT NULL,
  "score" numeric(5,2) NOT NULL,
  "component_scores" jsonb NULL,
  "matched_keywords" jsonb NULL,
  "passed" boolean NOT NULL,
  "config_hash" character varying(64) NULL,
  PRIMARY KEY ("crd_number", "pass", "scored_at"),
  CONSTRAINT "firm_scores_pass_check" CHECK ((pass >= 0) AND (pass <= 3))
);
-- Create index "idx_firm_scores_crd" to table: "firm_scores"
CREATE INDEX "idx_firm_scores_crd" ON "fed_data"."firm_scores" ("crd_number");
-- Create index "idx_firm_scores_latest" to table: "firm_scores"
CREATE INDEX "idx_firm_scores_latest" ON "fed_data"."firm_scores" ("crd_number", "pass", "scored_at" DESC);
-- Create index "idx_firm_scores_pass_score" to table: "firm_scores"
CREATE INDEX "idx_firm_scores_pass_score" ON "fed_data"."firm_scores" ("pass", "score" DESC);
-- Set comment to table: "firm_scores"
COMMENT ON TABLE "fed_data"."firm_scores" IS 'Multi-pass firm scoring results for M&A target identification funnel';
-- Set comment to column: "pass" on table: "firm_scores"
COMMENT ON COLUMN "fed_data"."firm_scores"."pass" IS '0=ADV, 1=Website, 2=T1 Extract, 3=Deep Enrichment';
-- Set comment to column: "component_scores" on table: "firm_scores"
COMMENT ON COLUMN "fed_data"."firm_scores"."component_scores" IS 'Individual scoring components (e.g., aum_fit, growth_score, client_quality)';
-- Set comment to column: "matched_keywords" on table: "firm_scores"
COMMENT ON COLUMN "fed_data"."firm_scores"."matched_keywords" IS 'Keywords that matched during scoring (geo, industry, succession)';
-- Set comment to column: "config_hash" on table: "firm_scores"
COMMENT ON COLUMN "fed_data"."firm_scores"."config_hash" IS 'SHA-256 of scoring config for reproducibility tracking';
