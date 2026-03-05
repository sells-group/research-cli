-- +goose Up
-- Baseline migration: extensions + public + fed_data schemas.
-- All statements use IF NOT EXISTS for idempotent application.


CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "postgis_tiger_geocoder";

-- Add new schema named "public"
CREATE SCHEMA IF NOT EXISTS "public";
-- Set comment to schema: "public"
COMMENT ON SCHEMA "public" IS 'standard public schema';
-- Create "linkedin_cache" table
CREATE TABLE IF NOT EXISTS "public"."linkedin_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "domain" text NOT NULL,
  "data" jsonb NOT NULL,
  "cached_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "linkedin_cache_domain_key" UNIQUE ("domain")
);
-- Create index "idx_linkedin_cache_domain" to table: "linkedin_cache"
CREATE INDEX IF NOT EXISTS "idx_linkedin_cache_domain" ON "public"."linkedin_cache" ("domain");
-- Create index "idx_linkedin_cache_expires_at" to table: "linkedin_cache"
CREATE INDEX IF NOT EXISTS "idx_linkedin_cache_expires_at" ON "public"."linkedin_cache" ("expires_at");
-- Create "geocode_cache" table
CREATE TABLE IF NOT EXISTS "public"."geocode_cache" (
  "address_hash" character varying(64) NOT NULL,
  "latitude" numeric(9,6) NOT NULL,
  "longitude" numeric(9,6) NOT NULL,
  "quality" character varying(20) NOT NULL,
  "rating" integer NULL,
  "cached_at" timestamptz NOT NULL DEFAULT now(),
  "matched" boolean NOT NULL DEFAULT true,
  "county_fips" character varying(5) NULL,
  PRIMARY KEY ("address_hash")
);
-- Create index "idx_geocode_cache_at" to table: "geocode_cache"
CREATE INDEX IF NOT EXISTS "idx_geocode_cache_at" ON "public"."geocode_cache" ("cached_at");
-- Set comment to table: "geocode_cache"
COMMENT ON TABLE "public"."geocode_cache" IS 'Caches PostGIS geocode() results keyed by SHA-256 of normalized address';
-- Create "checkpoints" table
CREATE TABLE IF NOT EXISTS "public"."checkpoints" (
  "company_id" text NOT NULL,
  "phase" text NOT NULL,
  "data" jsonb NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("company_id")
);
-- Create "scrape_cache" table
CREATE TABLE IF NOT EXISTS "public"."scrape_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "url_hash" text NOT NULL,
  "content" jsonb NOT NULL,
  "cached_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "scrape_cache_url_hash_key" UNIQUE ("url_hash")
);
-- Create index "idx_scrape_cache_expires_at" to table: "scrape_cache"
CREATE INDEX IF NOT EXISTS "idx_scrape_cache_expires_at" ON "public"."scrape_cache" ("expires_at");
-- Create index "idx_scrape_cache_url_hash" to table: "scrape_cache"
CREATE INDEX IF NOT EXISTS "idx_scrape_cache_url_hash" ON "public"."scrape_cache" ("url_hash");
-- Create "runs" table
CREATE TABLE IF NOT EXISTS "public"."runs" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "company" jsonb NOT NULL,
  "status" text NOT NULL DEFAULT 'queued',
  "result" jsonb NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_runs_status" to table: "runs"
CREATE INDEX IF NOT EXISTS "idx_runs_status" ON "public"."runs" ("status");
-- Create "premium_queries" table
CREATE TABLE IF NOT EXISTS "public"."premium_queries" (
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
CREATE INDEX IF NOT EXISTS "idx_premium_queries_company" ON "public"."premium_queries" ("company_url");
-- Create index "idx_premium_queries_provider" to table: "premium_queries"
CREATE INDEX IF NOT EXISTS "idx_premium_queries_provider" ON "public"."premium_queries" ("provider");
-- Create "crawl_cache" table
CREATE TABLE IF NOT EXISTS "public"."crawl_cache" (
  "id" text NOT NULL DEFAULT (gen_random_uuid())::text,
  "company_url" text NOT NULL,
  "pages" jsonb NOT NULL,
  "crawled_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "crawl_cache_company_url_key" UNIQUE ("company_url")
);
-- Create index "idx_crawl_cache_company_url" to table: "crawl_cache"
CREATE INDEX IF NOT EXISTS "idx_crawl_cache_company_url" ON "public"."crawl_cache" ("company_url");
-- Create index "idx_crawl_cache_expires_at" to table: "crawl_cache"
CREATE INDEX IF NOT EXISTS "idx_crawl_cache_expires_at" ON "public"."crawl_cache" ("expires_at");
-- Create index "idx_crawl_cache_url_expires" to table: "crawl_cache"
CREATE INDEX IF NOT EXISTS "idx_crawl_cache_url_expires" ON "public"."crawl_cache" ("company_url", "expires_at" DESC);
-- Create "companies" table
CREATE TABLE IF NOT EXISTS "public"."companies" (
  "id" bigserial NOT NULL,
  "name" character varying(300) NOT NULL,
  "legal_name" character varying(300) NULL,
  "domain" character varying(255) NOT NULL,
  "website" character varying(500) NULL,
  "description" text NULL,
  "naics_code" character varying(6) NULL,
  "sic_code" character varying(4) NULL,
  "business_model" character varying(100) NULL,
  "year_founded" smallint NULL,
  "ownership_type" character varying(50) NULL,
  "phone" character varying(30) NULL,
  "email" character varying(254) NULL,
  "employee_count" integer NULL,
  "employee_estimate" integer NULL,
  "revenue_estimate" bigint NULL,
  "revenue_range" character varying(50) NULL,
  "revenue_confidence" numeric(3,2) NULL,
  "street" character varying(300) NULL,
  "city" character varying(100) NULL,
  "state" character varying(10) NULL,
  "zip_code" character varying(20) NULL,
  "country" character varying(50) NULL DEFAULT 'US',
  "enrichment_score" numeric(4,2) NULL,
  "last_enriched_at" timestamptz NULL,
  "last_run_id" text NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  "services_list" text NULL,
  "service_area" character varying(500) NULL,
  "licenses_text" text NULL,
  "owner_name" character varying(255) NULL,
  "customer_types" character varying(500) NULL,
  "differentiators" text NULL,
  "reputation_summary" text NULL,
  "acquisition_assessment" text NULL,
  "key_people" text NULL,
  "exec_first_name" character varying(100) NULL,
  "exec_last_name" character varying(100) NULL,
  "exec_title" character varying(200) NULL,
  "exec_linkedin" character varying(500) NULL,
  "review_count" integer NULL,
  "review_rating" numeric(3,2) NULL,
  "employees_linkedin" integer NULL,
  "location_count" integer NULL,
  "end_markets" character varying(1000) NULL,
  "linkedin_url" character varying(500) NULL,
  "enrichment_report" text NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "companies_last_run_id_fkey" FOREIGN KEY ("last_run_id") REFERENCES "public"."runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_companies_domain" to table: "companies"
CREATE UNIQUE INDEX IF NOT EXISTS "idx_companies_domain" ON "public"."companies" ("domain") WHERE ((domain)::text <> ''::text);
-- Create index "idx_companies_naics" to table: "companies"
CREATE INDEX IF NOT EXISTS "idx_companies_naics" ON "public"."companies" ("naics_code");
-- Create index "idx_companies_name_trgm" to table: "companies"
CREATE INDEX IF NOT EXISTS "idx_companies_name_trgm" ON "public"."companies" USING GIN ("name" public.gin_trgm_ops);
-- Create index "idx_companies_state" to table: "companies"
CREATE INDEX IF NOT EXISTS "idx_companies_state" ON "public"."companies" ("state");
-- Create "company_addresses" table
CREATE TABLE IF NOT EXISTS "public"."company_addresses" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "address_type" character varying(30) NOT NULL,
  "street" character varying(300) NULL,
  "city" character varying(100) NULL,
  "state" character varying(10) NULL,
  "zip_code" character varying(20) NULL,
  "country" character varying(50) NULL DEFAULT 'US',
  "latitude" numeric(9,6) NULL,
  "longitude" numeric(9,6) NULL,
  "source" character varying(50) NULL,
  "confidence" numeric(3,2) NULL,
  "is_primary" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  "geom" public.geometry(Point,4326) NULL,
  "geocode_source" character varying(20) NULL,
  "geocode_quality" character varying(20) NULL,
  "geocoded_at" timestamptz NULL,
  "county_fips" character varying(5) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "company_addresses_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_addresses_company" to table: "company_addresses"
CREATE INDEX IF NOT EXISTS "idx_company_addresses_company" ON "public"."company_addresses" ("company_id");
-- Create index "idx_company_addresses_geom" to table: "company_addresses"
CREATE INDEX IF NOT EXISTS "idx_company_addresses_geom" ON "public"."company_addresses" USING GIST ("geom") WHERE (geom IS NOT NULL);
-- Create index "idx_company_addresses_state" to table: "company_addresses"
CREATE INDEX IF NOT EXISTS "idx_company_addresses_state" ON "public"."company_addresses" ("state");
-- Create "cbsa_areas" table
CREATE TABLE IF NOT EXISTS "public"."cbsa_areas" (
  "gid" serial NOT NULL,
  "cbsa_code" character varying(5) NOT NULL,
  "name" character varying(200) NOT NULL,
  "lsad" character varying(2) NULL,
  "geom" public.geometry(MultiPolygon,4326) NOT NULL,
  PRIMARY KEY ("gid"),
  CONSTRAINT "cbsa_areas_cbsa_code_key" UNIQUE ("cbsa_code")
);
-- Create index "idx_cbsa_code" to table: "cbsa_areas"
CREATE INDEX IF NOT EXISTS "idx_cbsa_code" ON "public"."cbsa_areas" ("cbsa_code");
-- Create index "idx_cbsa_geom" to table: "cbsa_areas"
CREATE INDEX IF NOT EXISTS "idx_cbsa_geom" ON "public"."cbsa_areas" USING GIST ("geom");
-- Create "address_msa" table
CREATE TABLE IF NOT EXISTS "public"."address_msa" (
  "id" bigserial NOT NULL,
  "address_id" bigint NOT NULL,
  "cbsa_code" character varying(5) NOT NULL,
  "is_within" boolean NOT NULL,
  "distance_km" numeric(8,2) NOT NULL,
  "centroid_km" numeric(8,2) NOT NULL,
  "edge_km" numeric(8,2) NOT NULL,
  "classification" character varying(20) NOT NULL,
  "computed_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "address_msa_address_id_cbsa_code_key" UNIQUE ("address_id", "cbsa_code"),
  CONSTRAINT "address_msa_address_id_fkey" FOREIGN KEY ("address_id") REFERENCES "public"."company_addresses" ("id") ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT "address_msa_cbsa_code_fkey" FOREIGN KEY ("cbsa_code") REFERENCES "public"."cbsa_areas" ("cbsa_code") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_addr_msa_address" to table: "address_msa"
CREATE INDEX IF NOT EXISTS "idx_addr_msa_address" ON "public"."address_msa" ("address_id");
-- Create index "idx_addr_msa_cbsa" to table: "address_msa"
CREATE INDEX IF NOT EXISTS "idx_addr_msa_cbsa" ON "public"."address_msa" ("cbsa_code");
-- Create index "idx_addr_msa_class" to table: "address_msa"
CREATE INDEX IF NOT EXISTS "idx_addr_msa_class" ON "public"."address_msa" ("classification");
-- Create "company_financials" table
CREATE TABLE IF NOT EXISTS "public"."company_financials" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "period_type" character varying(20) NOT NULL,
  "period_date" date NOT NULL,
  "metric" character varying(50) NOT NULL,
  "value" numeric(18,2) NULL,
  "source" character varying(50) NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "company_financials_company_id_period_type_period_date_metri_key" UNIQUE ("company_id", "period_type", "period_date", "metric", "source"),
  CONSTRAINT "company_financials_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_financials_company" to table: "company_financials"
CREATE INDEX IF NOT EXISTS "idx_company_financials_company" ON "public"."company_financials" ("company_id", "metric");
-- Create index "idx_company_financials_period" to table: "company_financials"
CREATE INDEX IF NOT EXISTS "idx_company_financials_period" ON "public"."company_financials" ("period_date" DESC);
-- Create "company_identifiers" table
CREATE TABLE IF NOT EXISTS "public"."company_identifiers" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "system" character varying(50) NOT NULL,
  "identifier" character varying(200) NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "company_identifiers_company_id_system_identifier_key" UNIQUE ("company_id", "system", "identifier"),
  CONSTRAINT "company_identifiers_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_identifiers_lookup" to table: "company_identifiers"
CREATE INDEX IF NOT EXISTS "idx_company_identifiers_lookup" ON "public"."company_identifiers" ("system", "identifier");
-- Create "company_matches" table
CREATE TABLE IF NOT EXISTS "public"."company_matches" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "matched_source" character varying(50) NOT NULL,
  "matched_key" character varying(200) NOT NULL,
  "match_type" character varying(30) NOT NULL,
  "confidence" numeric(3,2) NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "company_matches_company_id_matched_source_matched_key_key" UNIQUE ("company_id", "matched_source", "matched_key"),
  CONSTRAINT "company_matches_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_matches_source" to table: "company_matches"
CREATE INDEX IF NOT EXISTS "idx_company_matches_source" ON "public"."company_matches" ("matched_source", "matched_key");
-- Create "company_sources" table
CREATE TABLE IF NOT EXISTS "public"."company_sources" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "source" character varying(50) NOT NULL,
  "source_id" character varying(200) NULL,
  "raw_data" jsonb NULL,
  "extracted_fields" jsonb NULL,
  "data_as_of" timestamptz NULL,
  "fetched_at" timestamptz NOT NULL DEFAULT now(),
  "run_id" text NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "company_sources_company_id_source_source_id_key" UNIQUE ("company_id", "source", "source_id"),
  CONSTRAINT "company_sources_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT "company_sources_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_company_sources_company" to table: "company_sources"
CREATE INDEX IF NOT EXISTS "idx_company_sources_company" ON "public"."company_sources" ("company_id");
-- Create index "idx_company_sources_raw" to table: "company_sources"
CREATE INDEX IF NOT EXISTS "idx_company_sources_raw" ON "public"."company_sources" USING GIN ("raw_data");
-- Create "company_tags" table
CREATE TABLE IF NOT EXISTS "public"."company_tags" (
  "company_id" bigint NOT NULL,
  "tag_type" character varying(50) NOT NULL,
  "tag_value" character varying(200) NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("company_id", "tag_type", "tag_value"),
  CONSTRAINT "company_tags_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_tags_type" to table: "company_tags"
CREATE INDEX IF NOT EXISTS "idx_company_tags_type" ON "public"."company_tags" ("tag_type", "tag_value");
-- Create "contacts" table
CREATE TABLE IF NOT EXISTS "public"."contacts" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "first_name" character varying(100) NULL,
  "last_name" character varying(100) NULL,
  "full_name" character varying(200) NULL,
  "title" character varying(200) NULL,
  "role_type" character varying(30) NULL,
  "email" character varying(254) NULL,
  "phone" character varying(30) NULL,
  "linkedin_url" character varying(500) NULL,
  "ownership_pct" numeric(5,2) NULL,
  "is_control_person" boolean NULL DEFAULT false,
  "is_primary" boolean NULL DEFAULT false,
  "source" character varying(50) NULL,
  "confidence" numeric(3,2) NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "contacts_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_contacts_company_role" to table: "contacts"
CREATE INDEX IF NOT EXISTS "idx_contacts_company_role" ON "public"."contacts" ("company_id", "role_type");
-- Create index "idx_contacts_name_trgm" to table: "contacts"
CREATE INDEX IF NOT EXISTS "idx_contacts_name_trgm" ON "public"."contacts" USING GIN ("full_name" public.gin_trgm_ops);
-- Create "discovery_runs" table
CREATE TABLE IF NOT EXISTS "public"."discovery_runs" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "strategy" character varying(20) NOT NULL,
  "status" character varying(20) NOT NULL DEFAULT 'running',
  "config" jsonb NOT NULL,
  "candidates_found" integer NULL DEFAULT 0,
  "candidates_qualified" integer NULL DEFAULT 0,
  "cost_usd" numeric(10,4) NULL DEFAULT 0,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "completed_at" timestamptz NULL,
  "error" text NULL,
  PRIMARY KEY ("id")
);
-- Create "discovery_candidates" table
CREATE TABLE IF NOT EXISTS "public"."discovery_candidates" (
  "id" bigserial NOT NULL,
  "run_id" uuid NOT NULL,
  "google_place_id" character varying(200) NULL,
  "name" text NOT NULL,
  "domain" character varying(255) NULL,
  "website" character varying(500) NULL,
  "street" character varying(300) NULL,
  "city" character varying(100) NULL,
  "state" character varying(10) NULL,
  "zip_code" character varying(20) NULL,
  "naics_code" character varying(6) NULL,
  "source" character varying(20) NOT NULL,
  "source_record" jsonb NULL,
  "disqualified" boolean NULL DEFAULT false,
  "disqualify_reason" character varying(100) NULL,
  "score_t0" numeric(4,2) NULL,
  "score_t1" numeric(4,2) NULL,
  "score_t2" numeric(4,2) NULL,
  "promoted_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "discovery_candidates_run_id_google_place_id_key" UNIQUE ("run_id", "google_place_id"),
  CONSTRAINT "discovery_candidates_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."discovery_runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_disc_cand_domain" to table: "discovery_candidates"
CREATE INDEX IF NOT EXISTS "idx_disc_cand_domain" ON "public"."discovery_candidates" ("domain") WHERE (domain IS NOT NULL);
-- Create index "idx_disc_cand_place" to table: "discovery_candidates"
CREATE INDEX IF NOT EXISTS "idx_disc_cand_place" ON "public"."discovery_candidates" ("google_place_id") WHERE (google_place_id IS NOT NULL);
-- Create index "idx_disc_cand_run" to table: "discovery_candidates"
CREATE INDEX IF NOT EXISTS "idx_disc_cand_run" ON "public"."discovery_candidates" ("run_id", "disqualified");
-- Create "field_provenance" table
CREATE TABLE IF NOT EXISTS "public"."field_provenance" (
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
CREATE INDEX IF NOT EXISTS "idx_field_provenance_company" ON "public"."field_provenance" ("company_url", "field_key");
-- Create index "idx_field_provenance_run" to table: "field_provenance"
CREATE INDEX IF NOT EXISTS "idx_field_provenance_run" ON "public"."field_provenance" ("run_id");
-- Create "licenses" table
CREATE TABLE IF NOT EXISTS "public"."licenses" (
  "id" bigserial NOT NULL,
  "company_id" bigint NOT NULL,
  "license_type" character varying(100) NOT NULL,
  "license_number" character varying(100) NULL,
  "authority" character varying(200) NULL,
  "state" character varying(10) NULL,
  "status" character varying(50) NULL,
  "issued_date" date NULL,
  "expiry_date" date NULL,
  "source" character varying(50) NULL,
  "raw_text" text NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "licenses_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_licenses_company" to table: "licenses"
CREATE INDEX IF NOT EXISTS "idx_licenses_company" ON "public"."licenses" ("company_id");
-- Create index "idx_licenses_type_state" to table: "licenses"
CREATE INDEX IF NOT EXISTS "idx_licenses_type_state" ON "public"."licenses" ("license_type", "state");
-- Create "msa_grid_cells" table
CREATE TABLE IF NOT EXISTS "public"."msa_grid_cells" (
  "id" bigserial NOT NULL,
  "cbsa_code" character varying(5) NOT NULL,
  "cell_km" numeric(4,1) NOT NULL,
  "sw_lat" numeric(9,6) NOT NULL,
  "sw_lon" numeric(9,6) NOT NULL,
  "ne_lat" numeric(9,6) NOT NULL,
  "ne_lon" numeric(9,6) NOT NULL,
  "geom" public.geometry(Polygon,4326) NOT NULL,
  "searched_at" timestamptz NULL,
  "result_count" integer NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "msa_grid_cells_cbsa_code_cell_km_sw_lat_sw_lon_key" UNIQUE ("cbsa_code", "cell_km", "sw_lat", "sw_lon"),
  CONSTRAINT "msa_grid_cells_cbsa_code_fkey" FOREIGN KEY ("cbsa_code") REFERENCES "public"."cbsa_areas" ("cbsa_code") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_grid_cbsa" to table: "msa_grid_cells"
CREATE INDEX IF NOT EXISTS "idx_grid_cbsa" ON "public"."msa_grid_cells" ("cbsa_code", "cell_km");
-- Create index "idx_grid_geom" to table: "msa_grid_cells"
CREATE INDEX IF NOT EXISTS "idx_grid_geom" ON "public"."msa_grid_cells" USING GIST ("geom");
-- Create index "idx_grid_unsearched" to table: "msa_grid_cells"
CREATE INDEX IF NOT EXISTS "idx_grid_unsearched" ON "public"."msa_grid_cells" ("cbsa_code") WHERE (searched_at IS NULL);
-- Create "run_phases" table
CREATE TABLE IF NOT EXISTS "public"."run_phases" (
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
CREATE INDEX IF NOT EXISTS "idx_run_phases_run_id" ON "public"."run_phases" ("run_id");

-- Add new schema named "fed_data"
CREATE SCHEMA IF NOT EXISTS "fed_data";
-- Create "nes_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."nes_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "firmpdemp" integer NULL,
  "rcppdemp" bigint NULL,
  "payann_pct" numeric(8,2) NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);
-- Create "fips_codes" table
CREATE TABLE IF NOT EXISTS "fed_data"."fips_codes" (
  "fips_state" character(2) NOT NULL,
  "fips_county" character(3) NOT NULL DEFAULT '000',
  "state_name" character varying(50) NULL,
  "county_name" character varying(100) NULL,
  "state_abbr" character(2) NULL,
  "ansi_code" character varying(10) NULL,
  "aland" bigint NULL,
  "awater" bigint NULL,
  "aland_sqmi" numeric(12,2) NULL,
  "awater_sqmi" numeric(12,2) NULL,
  "intptlat" numeric(11,7) NULL,
  "intptlong" numeric(12,7) NULL,
  "updated_at" timestamptz NULL,
  PRIMARY KEY ("fips_state", "fips_county")
);
-- Create index "idx_fips_abbr" to table: "fips_codes"
CREATE INDEX IF NOT EXISTS "idx_fips_abbr" ON "fed_data"."fips_codes" ("state_abbr");
-- Create index "idx_fips_geoid" to table: "fips_codes"
CREATE INDEX IF NOT EXISTS "idx_fips_geoid" ON "fed_data"."fips_codes" ((((fips_state)::text || (fips_county)::text)));
-- Create index "idx_fips_state" to table: "fips_codes"
CREATE INDEX IF NOT EXISTS "idx_fips_state" ON "fed_data"."fips_codes" ("fips_state");
-- Create "xbrl_facts" table
CREATE TABLE IF NOT EXISTS "fed_data"."xbrl_facts" (
  "cik" character varying(10) NOT NULL,
  "fact_name" character varying(100) NOT NULL,
  "period_end" date NOT NULL,
  "value" numeric NULL,
  "unit" character varying(30) NULL,
  "form" character varying(10) NULL,
  "fy" smallint NULL,
  "accession" character varying(25) NULL,
  PRIMARY KEY ("cik", "fact_name", "period_end")
);
-- Create index "idx_xbrl_cik_fact" to table: "xbrl_facts"
CREATE INDEX IF NOT EXISTS "idx_xbrl_cik_fact" ON "fed_data"."xbrl_facts" ("cik", "fact_name");
-- Create index "idx_xbrl_facts_cik" to table: "xbrl_facts"
CREATE INDEX IF NOT EXISTS "idx_xbrl_facts_cik" ON "fed_data"."xbrl_facts" ("cik");
-- Create "adv_answer_history" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_answer_history" (
  "id" bigserial NOT NULL,
  "crd_number" integer NOT NULL,
  "fund_id" character varying(20) NULL,
  "question_key" character varying(80) NOT NULL,
  "value" jsonb NULL,
  "confidence" numeric(3,2) NULL,
  "tier" smallint NULL,
  "reasoning" text NULL,
  "source_doc" character varying(20) NULL,
  "source_section" character varying(50) NULL,
  "model" character varying(50) NULL,
  "run_id" bigint NULL,
  "superseded_at" timestamptz NOT NULL DEFAULT now(),
  "superseded_by" bigint NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_answer_hist_crd" to table: "adv_answer_history"
CREATE INDEX IF NOT EXISTS "idx_answer_hist_crd" ON "fed_data"."adv_answer_history" ("crd_number", "question_key");
-- Create "adv_bd_affiliations" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_bd_affiliations" (
  "crd_number" integer NOT NULL,
  "bd_name" character varying(200) NOT NULL,
  "bd_crd" integer NULL,
  "relationship" character varying(50) NULL DEFAULT 'affiliated',
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "bd_name")
);
-- Create index "idx_bd_aff_name" to table: "adv_bd_affiliations"
CREATE INDEX IF NOT EXISTS "idx_bd_aff_name" ON "fed_data"."adv_bd_affiliations" ("bd_name");
-- Create "adv_brochure_enrichment" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_brochure_enrichment" (
  "crd_number" integer NOT NULL,
  "brochure_id" character varying(50) NOT NULL,
  "investment_strategies" jsonb NULL,
  "industry_specializations" jsonb NULL,
  "min_account_size" bigint NULL,
  "fee_schedule" text NULL,
  "target_clients" text NULL,
  "model" character varying(50) NULL,
  "input_tokens" integer NULL,
  "output_tokens" integer NULL,
  "enriched_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "brochure_id")
);
-- Create index "idx_brochure_enrich_industries" to table: "adv_brochure_enrichment"
CREATE INDEX IF NOT EXISTS "idx_brochure_enrich_industries" ON "fed_data"."adv_brochure_enrichment" USING GIN ("industry_specializations");
-- Create index "idx_brochure_enrich_strategies" to table: "adv_brochure_enrichment"
CREATE INDEX IF NOT EXISTS "idx_brochure_enrich_strategies" ON "fed_data"."adv_brochure_enrichment" USING GIN ("investment_strategies");
-- Create "adv_brochures" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_brochures" (
  "crd_number" integer NOT NULL,
  "brochure_id" character varying(50) NOT NULL,
  "filing_date" date NULL,
  "text_content" text NULL,
  "extracted_at" timestamptz NULL,
  PRIMARY KEY ("crd_number", "brochure_id")
);
-- Create "adv_computed_metrics" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_computed_metrics" (
  "crd_number" integer NOT NULL,
  "revenue_estimate" bigint NULL,
  "blended_fee_rate_bps" integer NULL,
  "revenue_per_client" integer NULL,
  "aum_growth_cagr_pct" numeric(5,2) NULL,
  "client_growth_rate_pct" numeric(5,2) NULL,
  "employee_growth_rate_pct" numeric(5,2) NULL,
  "hnw_revenue_pct" numeric(5,2) NULL,
  "institutional_revenue_pct" numeric(5,2) NULL,
  "fund_aum_pct_total" numeric(5,2) NULL,
  "compensation_diversity" smallint NULL,
  "business_complexity" smallint NULL,
  "drp_severity" smallint NULL,
  "acquisition_readiness" smallint NULL,
  "computed_at" timestamptz NOT NULL DEFAULT now(),
  "aum_1yr_growth_pct" numeric(8,4) NULL,
  "aum_3yr_cagr_pct" numeric(8,4) NULL,
  "aum_5yr_cagr_pct" numeric(8,4) NULL,
  "client_3yr_cagr_pct" numeric(8,4) NULL,
  "employee_3yr_cagr_pct" numeric(8,4) NULL,
  "concentration_risk_score" smallint NULL,
  "key_person_dependency_score" smallint NULL,
  "hybrid_revenue_estimate" bigint NULL,
  "estimated_expense_ratio" numeric(5,4) NULL,
  "estimated_operating_margin" numeric(5,4) NULL,
  "revenue_per_employee" bigint NULL,
  "benchmark_aum_per_employee_pctile" numeric(5,2) NULL,
  "benchmark_fee_rate_pctile" numeric(5,2) NULL,
  "amendments_last_year" integer NULL,
  "amendments_per_year_avg" numeric(5,2) NULL,
  "has_frequent_amendments" boolean NULL DEFAULT false,
  "regulatory_risk_score" smallint NULL,
  PRIMARY KEY ("crd_number")
);
-- Create "adv_crs" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_crs" (
  "crd_number" integer NOT NULL,
  "crs_id" character varying(50) NOT NULL,
  "filing_date" date NULL,
  "text_content" text NULL,
  "extracted_at" timestamptz NULL,
  PRIMARY KEY ("crd_number", "crs_id")
);
-- Create "firm_scores" table
CREATE TABLE IF NOT EXISTS "fed_data"."firm_scores" (
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
CREATE INDEX IF NOT EXISTS "idx_firm_scores_crd" ON "fed_data"."firm_scores" ("crd_number");
-- Create index "idx_firm_scores_latest" to table: "firm_scores"
CREATE INDEX IF NOT EXISTS "idx_firm_scores_latest" ON "fed_data"."firm_scores" ("crd_number", "pass", "scored_at" DESC);
-- Create index "idx_firm_scores_pass_score" to table: "firm_scores"
CREATE INDEX IF NOT EXISTS "idx_firm_scores_pass_score" ON "fed_data"."firm_scores" ("pass", "score" DESC);
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
-- Create "adv_custodian_relationships" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_custodian_relationships" (
  "crd_number" integer NOT NULL,
  "custodian_name" character varying(200) NOT NULL,
  "relationship" character varying(50) NULL DEFAULT 'custodian',
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "custodian_name")
);
-- Create index "idx_custodian_rel_name" to table: "adv_custodian_relationships"
CREATE INDEX IF NOT EXISTS "idx_custodian_rel_name" ON "fed_data"."adv_custodian_relationships" ("custodian_name");
-- Create "adv_disclosures" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_disclosures" (
  "crd_number" integer NOT NULL,
  "disclosure_type" character varying(100) NOT NULL,
  "event_date" date NULL,
  "description" text NULL,
  "id" bigserial NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_adv_disclosures_crd" to table: "adv_disclosures"
CREATE INDEX IF NOT EXISTS "idx_adv_disclosures_crd" ON "fed_data"."adv_disclosures" ("crd_number");
-- Create "adv_document_sections" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_document_sections" (
  "crd_number" integer NOT NULL,
  "doc_type" character varying(10) NOT NULL,
  "doc_id" character varying(50) NOT NULL,
  "section_key" character varying(20) NOT NULL,
  "section_title" character varying(200) NULL,
  "char_length" integer NULL,
  "token_estimate" integer NULL,
  "indexed_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "doc_type", "doc_id", "section_key")
);
-- Create "adv_extraction_runs" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_extraction_runs" (
  "id" bigserial NOT NULL,
  "crd_number" integer NOT NULL,
  "scope" character varying(20) NOT NULL DEFAULT 'advisor',
  "fund_id" character varying(20) NULL,
  "status" character varying(20) NOT NULL DEFAULT 'pending',
  "tier_completed" smallint NOT NULL DEFAULT 0,
  "total_questions" integer NOT NULL DEFAULT 0,
  "answered" integer NOT NULL DEFAULT 0,
  "input_tokens" integer NOT NULL DEFAULT 0,
  "output_tokens" integer NOT NULL DEFAULT 0,
  "cost_usd" numeric(8,4) NOT NULL DEFAULT 0,
  "error_message" text NULL,
  "started_at" timestamptz NULL,
  "completed_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_adv_extraction_runs_crd" to table: "adv_extraction_runs"
CREATE INDEX IF NOT EXISTS "idx_adv_extraction_runs_crd" ON "fed_data"."adv_extraction_runs" ("crd_number");
-- Create index "idx_adv_extraction_runs_status" to table: "adv_extraction_runs"
CREATE INDEX IF NOT EXISTS "idx_adv_extraction_runs_status" ON "fed_data"."adv_extraction_runs" ("status");
-- Create "adv_filings" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_filings" (
  "crd_number" integer NOT NULL,
  "filing_date" date NOT NULL,
  "aum" bigint NULL,
  "raum" bigint NULL,
  "num_accounts" integer NULL,
  "num_employees" integer NULL,
  "legal_name" character varying(300) NULL,
  "form_of_org" character varying(100) NULL,
  "num_other_offices" integer NULL,
  "total_employees" integer NULL,
  "num_adviser_reps" integer NULL,
  "client_types" jsonb NULL,
  "comp_pct_aum" boolean NULL DEFAULT false,
  "comp_hourly" boolean NULL DEFAULT false,
  "comp_subscription" boolean NULL DEFAULT false,
  "comp_fixed" boolean NULL DEFAULT false,
  "comp_commissions" boolean NULL DEFAULT false,
  "comp_performance" boolean NULL DEFAULT false,
  "comp_other" boolean NULL DEFAULT false,
  "aum_discretionary" bigint NULL,
  "aum_non_discretionary" bigint NULL,
  "aum_total" bigint NULL,
  "svc_financial_planning" boolean NULL DEFAULT false,
  "svc_portfolio_individuals" boolean NULL DEFAULT false,
  "svc_portfolio_inv_cos" boolean NULL DEFAULT false,
  "svc_portfolio_pooled" boolean NULL DEFAULT false,
  "svc_portfolio_institutional" boolean NULL DEFAULT false,
  "svc_pension_consulting" boolean NULL DEFAULT false,
  "svc_adviser_selection" boolean NULL DEFAULT false,
  "svc_periodicals" boolean NULL DEFAULT false,
  "svc_security_ratings" boolean NULL DEFAULT false,
  "svc_market_timing" boolean NULL DEFAULT false,
  "svc_seminars" boolean NULL DEFAULT false,
  "svc_other" boolean NULL DEFAULT false,
  "wrap_fee_program" boolean NULL DEFAULT false,
  "wrap_fee_raum" bigint NULL,
  "financial_planning_clients" integer NULL,
  "biz_broker_dealer" boolean NULL DEFAULT false,
  "biz_registered_rep" boolean NULL DEFAULT false,
  "biz_cpo_cta" boolean NULL DEFAULT false,
  "biz_futures_commission" boolean NULL DEFAULT false,
  "biz_real_estate" boolean NULL DEFAULT false,
  "biz_insurance" boolean NULL DEFAULT false,
  "biz_bank" boolean NULL DEFAULT false,
  "biz_trust_company" boolean NULL DEFAULT false,
  "biz_municipal_advisor" boolean NULL DEFAULT false,
  "biz_swap_dealer" boolean NULL DEFAULT false,
  "biz_major_swap" boolean NULL DEFAULT false,
  "biz_accountant" boolean NULL DEFAULT false,
  "biz_lawyer" boolean NULL DEFAULT false,
  "biz_other_financial" boolean NULL DEFAULT false,
  "aff_broker_dealer" boolean NULL DEFAULT false,
  "aff_other_adviser" boolean NULL DEFAULT false,
  "aff_municipal_advisor" boolean NULL DEFAULT false,
  "aff_swap_dealer" boolean NULL DEFAULT false,
  "aff_major_swap" boolean NULL DEFAULT false,
  "aff_cpo_cta" boolean NULL DEFAULT false,
  "aff_futures_commission" boolean NULL DEFAULT false,
  "aff_bank" boolean NULL DEFAULT false,
  "aff_trust_company" boolean NULL DEFAULT false,
  "aff_accountant" boolean NULL DEFAULT false,
  "aff_lawyer" boolean NULL DEFAULT false,
  "aff_insurance" boolean NULL DEFAULT false,
  "aff_pension_consultant" boolean NULL DEFAULT false,
  "aff_real_estate" boolean NULL DEFAULT false,
  "aff_lp_sponsor" boolean NULL DEFAULT false,
  "aff_pooled_vehicle" boolean NULL DEFAULT false,
  "sec_registered" boolean NULL DEFAULT false,
  "exempt_reporting" boolean NULL DEFAULT false,
  "state_registered" boolean NULL DEFAULT false,
  "discretionary_authority" boolean NULL DEFAULT false,
  "txn_proprietary_interest" boolean NULL DEFAULT false,
  "txn_sells_own_securities" boolean NULL DEFAULT false,
  "txn_buys_from_clients" boolean NULL DEFAULT false,
  "txn_recommends_own" boolean NULL DEFAULT false,
  "txn_recommends_broker" boolean NULL DEFAULT false,
  "txn_agency_cross" boolean NULL DEFAULT false,
  "txn_principal" boolean NULL DEFAULT false,
  "txn_referral_compensation" boolean NULL DEFAULT false,
  "txn_other_research" boolean NULL DEFAULT false,
  "txn_revenue_sharing" boolean NULL DEFAULT false,
  "custody_client_cash" boolean NULL DEFAULT false,
  "custody_client_securities" boolean NULL DEFAULT false,
  "custody_related_person" boolean NULL DEFAULT false,
  "custody_qualified_custodian" boolean NULL DEFAULT false,
  "custody_surprise_exam" boolean NULL DEFAULT false,
  "drp_criminal_firm" boolean NULL DEFAULT false,
  "drp_criminal_affiliate" boolean NULL DEFAULT false,
  "drp_regulatory_firm" boolean NULL DEFAULT false,
  "drp_regulatory_affiliate" boolean NULL DEFAULT false,
  "drp_civil_firm" boolean NULL DEFAULT false,
  "drp_civil_affiliate" boolean NULL DEFAULT false,
  "drp_complaint_firm" boolean NULL DEFAULT false,
  "drp_complaint_affiliate" boolean NULL DEFAULT false,
  "drp_termination_firm" boolean NULL DEFAULT false,
  "drp_termination_affiliate" boolean NULL DEFAULT false,
  "drp_judgment" boolean NULL DEFAULT false,
  "drp_financial_firm" boolean NULL DEFAULT false,
  "drp_financial_affiliate" boolean NULL DEFAULT false,
  "has_any_drp" boolean NULL DEFAULT false,
  "updated_at" timestamptz NULL DEFAULT now(),
  "filing_type" character varying(20) NULL,
  PRIMARY KEY ("crd_number", "filing_date")
);
-- Create index "idx_adv_filings_aum" to table: "adv_filings"
CREATE INDEX IF NOT EXISTS "idx_adv_filings_aum" ON "fed_data"."adv_filings" ("aum" DESC NULLS LAST);
-- Create index "idx_adv_filings_date" to table: "adv_filings"
CREATE INDEX IF NOT EXISTS "idx_adv_filings_date" ON "fed_data"."adv_filings" ("filing_date" DESC);
-- Create index "idx_adv_filings_drp" to table: "adv_filings"
CREATE INDEX IF NOT EXISTS "idx_adv_filings_drp" ON "fed_data"."adv_filings" ("has_any_drp") WHERE (has_any_drp = true);
-- Set comment to column: "filing_type" on table: "adv_filings"
COMMENT ON COLUMN "fed_data"."adv_filings"."filing_type" IS 'Filing type: annual, amendment, initial, etc.';
-- Create "adv_firms" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_firms" (
  "crd_number" integer NOT NULL,
  "firm_name" character varying(200) NOT NULL,
  "sec_number" character varying(20) NULL,
  "city" character varying(100) NULL,
  "state" character varying(10) NULL,
  "country" character varying(50) NULL,
  "website" character varying(300) NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  "street1" character varying(200) NULL,
  "street2" character varying(200) NULL,
  "zip" character varying(10) NULL,
  PRIMARY KEY ("crd_number")
);
-- Create index "idx_adv_firms_name" to table: "adv_firms"
CREATE INDEX IF NOT EXISTS "idx_adv_firms_name" ON "fed_data"."adv_firms" USING GIN ("firm_name" public.gin_trgm_ops);
-- Create index "idx_adv_firms_state" to table: "adv_firms"
CREATE INDEX IF NOT EXISTS "idx_adv_firms_state" ON "fed_data"."adv_firms" ("state");
-- Create "usaspending_awards" table
CREATE TABLE IF NOT EXISTS "fed_data"."usaspending_awards" (
  "award_id" text NOT NULL,
  "award_type" text NOT NULL,
  "award_type_code" text NULL,
  "piid" text NULL,
  "fain" text NULL,
  "uri" text NULL,
  "awarding_agency_code" text NULL,
  "awarding_agency_name" text NULL,
  "awarding_sub_agency_code" text NULL,
  "awarding_sub_agency_name" text NULL,
  "funding_agency_code" text NULL,
  "funding_agency_name" text NULL,
  "recipient_uei" character varying(12) NULL,
  "recipient_duns" character varying(13) NULL,
  "recipient_name" text NULL,
  "recipient_parent_uei" character varying(12) NULL,
  "recipient_parent_name" text NULL,
  "recipient_address_line_1" text NULL,
  "recipient_city" text NULL,
  "recipient_state" character(2) NULL,
  "recipient_zip" text NULL,
  "recipient_country" text NULL,
  "total_obligated_amount" numeric(15,2) NULL,
  "total_outlayed_amount" numeric(15,2) NULL,
  "naics_code" character varying(6) NULL,
  "naics_description" text NULL,
  "psc_code" character varying(4) NULL,
  "cfda_number" text NULL,
  "cfda_title" text NULL,
  "award_base_action_date" date NULL,
  "award_latest_action_date" date NULL,
  "period_of_perf_start" date NULL,
  "period_of_perf_end" date NULL,
  "last_modified_date" date NULL,
  "pop_city" text NULL,
  "pop_state" character(2) NULL,
  "pop_zip" text NULL,
  "pop_country" text NULL,
  "award_description" text NULL,
  "usaspending_permalink" text NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("award_id")
);
-- Create index "idx_usaspending_action_date" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_action_date" ON "fed_data"."usaspending_awards" ("award_latest_action_date" DESC);
-- Create index "idx_usaspending_agency" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_agency" ON "fed_data"."usaspending_awards" ("awarding_agency_code");
-- Create index "idx_usaspending_cfda" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_cfda" ON "fed_data"."usaspending_awards" ("cfda_number");
-- Create index "idx_usaspending_modified" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_modified" ON "fed_data"."usaspending_awards" ("last_modified_date" DESC);
-- Create index "idx_usaspending_naics" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_naics" ON "fed_data"."usaspending_awards" ("naics_code");
-- Create index "idx_usaspending_name_trgm" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_name_trgm" ON "fed_data"."usaspending_awards" USING GIN ("recipient_name" public.gin_trgm_ops);
-- Create index "idx_usaspending_recipient_duns" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_recipient_duns" ON "fed_data"."usaspending_awards" ("recipient_duns");
-- Create index "idx_usaspending_recipient_uei" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_recipient_uei" ON "fed_data"."usaspending_awards" ("recipient_uei");
-- Create index "idx_usaspending_state" to table: "usaspending_awards"
CREATE INDEX IF NOT EXISTS "idx_usaspending_state" ON "fed_data"."usaspending_awards" ("recipient_state");
-- Create "adv_fund_filings" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_fund_filings" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(50) NOT NULL,
  "filing_date" date NOT NULL,
  "gross_asset_value" bigint NULL,
  "net_asset_value" bigint NULL,
  "fund_type" character varying(100) NULL,
  PRIMARY KEY ("crd_number", "fund_id", "filing_date")
);
-- Create "adv_fund_performance" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_fund_performance" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(50) NOT NULL,
  "metric_type" character varying(50) NOT NULL,
  "period" character varying(20) NOT NULL,
  "value_pct" numeric(8,4) NULL,
  "source" character varying(20) NULL,
  "extracted_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "fund_id", "metric_type", "period")
);
-- Create index "idx_fund_perf_crd" to table: "adv_fund_performance"
CREATE INDEX IF NOT EXISTS "idx_fund_perf_crd" ON "fed_data"."adv_fund_performance" ("crd_number");
-- Create "adv_owners" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_owners" (
  "crd_number" integer NOT NULL,
  "owner_name" character varying(200) NOT NULL,
  "owner_type" character varying(50) NULL,
  "ownership_pct" numeric(5,2) NULL,
  "is_control" boolean NULL DEFAULT false,
  PRIMARY KEY ("crd_number", "owner_name")
);
-- Create index "idx_adv_owners_name" to table: "adv_owners"
CREATE INDEX IF NOT EXISTS "idx_adv_owners_name" ON "fed_data"."adv_owners" USING GIN ("owner_name" public.gin_trgm_ops);
-- Create "adv_private_funds" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_private_funds" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(50) NOT NULL,
  "fund_name" character varying(300) NULL,
  "fund_type" character varying(100) NULL,
  "gross_asset_value" bigint NULL,
  "net_asset_value" bigint NULL,
  PRIMARY KEY ("crd_number", "fund_id")
);
-- Create "adv_service_providers" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_service_providers" (
  "crd_number" integer NOT NULL,
  "provider_name" character varying(200) NOT NULL,
  "provider_type" character varying(50) NOT NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "provider_name", "provider_type")
);
-- Create index "idx_svc_provider_type" to table: "adv_service_providers"
CREATE INDEX IF NOT EXISTS "idx_svc_provider_type" ON "fed_data"."adv_service_providers" ("provider_type");
-- Create "asm_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."asm_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "valadd" bigint NULL,
  "totval_ship" bigint NULL,
  "prodwrkrs" integer NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);
-- Create "brokercheck" table
CREATE TABLE IF NOT EXISTS "fed_data"."brokercheck" (
  "crd_number" integer NOT NULL,
  "firm_name" character varying(300) NOT NULL,
  "sec_number" character varying(20) NULL,
  "main_addr_city" character varying(100) NULL,
  "main_addr_state" character(2) NULL,
  "num_branch_offices" integer NULL,
  "num_registered_reps" integer NULL,
  "registration_status" character varying(50) NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  "has_disclosures" boolean NULL,
  "disclosure_count" integer NULL,
  PRIMARY KEY ("crd_number")
);
-- Create index "idx_brokercheck_crd" to table: "brokercheck"
CREATE INDEX IF NOT EXISTS "idx_brokercheck_crd" ON "fed_data"."brokercheck" ("crd_number");
-- Create index "idx_brokercheck_name" to table: "brokercheck"
CREATE INDEX IF NOT EXISTS "idx_brokercheck_name" ON "fed_data"."brokercheck" USING GIN ("firm_name" public.gin_trgm_ops);
-- Create "cbp_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."cbp_data" (
  "year" smallint NOT NULL,
  "fips_state" character(2) NOT NULL,
  "fips_county" character(3) NOT NULL,
  "naics" character varying(6) NOT NULL,
  "emp" integer NULL,
  "emp_nf" character(1) NULL,
  "qp1" bigint NULL,
  "qp1_nf" character(1) NULL,
  "ap" bigint NULL,
  "ap_nf" character(1) NULL,
  "est" integer NULL,
  PRIMARY KEY ("year", "fips_state", "fips_county", "naics")
);
-- Create index "idx_cbp_fips" to table: "cbp_data"
CREATE INDEX IF NOT EXISTS "idx_cbp_fips" ON "fed_data"."cbp_data" ("fips_state", "fips_county");
-- Create index "idx_cbp_naics" to table: "cbp_data"
CREATE INDEX IF NOT EXISTS "idx_cbp_naics" ON "fed_data"."cbp_data" ("naics");
-- Create "eci_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."eci_data" (
  "series_id" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "period" character varying(3) NOT NULL,
  "value" numeric(10,1) NULL,
  PRIMARY KEY ("series_id", "year", "period")
);
-- Create "economic_census" table
CREATE TABLE IF NOT EXISTS "fed_data"."economic_census" (
  "year" smallint NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "naics" character varying(6) NOT NULL,
  "estab" integer NULL,
  "rcptot" bigint NULL,
  "payann" bigint NULL,
  "emp" integer NULL,
  PRIMARY KEY ("year", "geo_id", "naics")
);
-- Create index "idx_econcensus_naics" to table: "economic_census"
CREATE INDEX IF NOT EXISTS "idx_econcensus_naics" ON "fed_data"."economic_census" ("naics");
-- Create "edgar_entities" table
CREATE TABLE IF NOT EXISTS "fed_data"."edgar_entities" (
  "cik" character varying(10) NOT NULL,
  "entity_name" character varying(200) NOT NULL,
  "entity_type" character varying(20) NULL,
  "sic" character varying(4) NULL,
  "sic_description" character varying(200) NULL,
  "state_of_inc" character varying(5) NULL,
  "state_of_business" character varying(5) NULL,
  "ein" character varying(10) NULL,
  "tickers" text[] NULL,
  "exchanges" text[] NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("cik")
);
-- Create index "idx_edgar_entities_name" to table: "edgar_entities"
CREATE INDEX IF NOT EXISTS "idx_edgar_entities_name" ON "fed_data"."edgar_entities" USING GIN ("entity_name" public.gin_trgm_ops);
-- Create index "idx_edgar_entities_sic" to table: "edgar_entities"
CREATE INDEX IF NOT EXISTS "idx_edgar_entities_sic" ON "fed_data"."edgar_entities" ("sic");
-- Create "edgar_filings" table
CREATE TABLE IF NOT EXISTS "fed_data"."edgar_filings" (
  "accession_number" character varying(25) NOT NULL,
  "cik" character varying(10) NOT NULL,
  "form_type" character varying(20) NOT NULL,
  "filing_date" date NOT NULL,
  "primary_doc" character varying(200) NULL,
  "primary_doc_desc" character varying(300) NULL,
  "items" text NULL,
  "size" integer NULL,
  "is_xbrl" boolean NULL DEFAULT false,
  "is_inline_xbrl" boolean NULL DEFAULT false,
  PRIMARY KEY ("accession_number")
);
-- Create index "idx_edgar_filings_cik" to table: "edgar_filings"
CREATE INDEX IF NOT EXISTS "idx_edgar_filings_cik" ON "fed_data"."edgar_filings" ("cik");
-- Create index "idx_edgar_filings_date" to table: "edgar_filings"
CREATE INDEX IF NOT EXISTS "idx_edgar_filings_date" ON "fed_data"."edgar_filings" ("filing_date");
-- Create index "idx_edgar_filings_date_form" to table: "edgar_filings"
CREATE INDEX IF NOT EXISTS "idx_edgar_filings_date_form" ON "fed_data"."edgar_filings" ("filing_date" DESC, "form_type");
-- Create index "idx_edgar_filings_form" to table: "edgar_filings"
CREATE INDEX IF NOT EXISTS "idx_edgar_filings_form" ON "fed_data"."edgar_filings" ("form_type");
-- Create "entity_xref" table
CREATE TABLE IF NOT EXISTS "fed_data"."entity_xref" (
  "id" bigserial NOT NULL,
  "crd_number" integer NULL,
  "cik" character varying(10) NULL,
  "entity_name" character varying(200) NULL,
  "match_type" character varying(20) NOT NULL,
  "confidence" numeric(3,2) NULL,
  "created_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_entity_xref_cik" to table: "entity_xref"
CREATE INDEX IF NOT EXISTS "idx_entity_xref_cik" ON "fed_data"."entity_xref" ("cik");
-- Create index "idx_entity_xref_crd" to table: "entity_xref"
CREATE INDEX IF NOT EXISTS "idx_entity_xref_crd" ON "fed_data"."entity_xref" ("crd_number");
-- Create index "idx_entity_xref_crd_cik" to table: "entity_xref"
CREATE UNIQUE INDEX IF NOT EXISTS "idx_entity_xref_crd_cik" ON "fed_data"."entity_xref" ("crd_number", "cik") WHERE ((crd_number IS NOT NULL) AND (cik IS NOT NULL));
-- Create index "idx_entity_xref_match" to table: "entity_xref"
CREATE INDEX IF NOT EXISTS "idx_entity_xref_match" ON "fed_data"."entity_xref" ("match_type", "confidence" DESC);
-- Create "entity_xref_multi" table
CREATE TABLE IF NOT EXISTS "fed_data"."entity_xref_multi" (
  "id" bigserial NOT NULL,
  "source_dataset" character varying(30) NOT NULL,
  "source_id" text NOT NULL,
  "target_dataset" character varying(30) NOT NULL,
  "target_id" text NOT NULL,
  "entity_name" character varying(300) NULL,
  "match_type" character varying(30) NOT NULL,
  "confidence" numeric(3,2) NOT NULL,
  "created_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
-- Create index "idx_xref_multi_confidence" to table: "entity_xref_multi"
CREATE INDEX IF NOT EXISTS "idx_xref_multi_confidence" ON "fed_data"."entity_xref_multi" ("confidence" DESC);
-- Create index "idx_xref_multi_match_type" to table: "entity_xref_multi"
CREATE INDEX IF NOT EXISTS "idx_xref_multi_match_type" ON "fed_data"."entity_xref_multi" ("match_type");
-- Create index "idx_xref_multi_pair" to table: "entity_xref_multi"
CREATE UNIQUE INDEX IF NOT EXISTS "idx_xref_multi_pair" ON "fed_data"."entity_xref_multi" ("source_dataset", "source_id", "target_dataset", "target_id");
-- Create index "idx_xref_multi_source" to table: "entity_xref_multi"
CREATE INDEX IF NOT EXISTS "idx_xref_multi_source" ON "fed_data"."entity_xref_multi" ("source_dataset", "source_id");
-- Create index "idx_xref_multi_target" to table: "entity_xref_multi"
CREATE INDEX IF NOT EXISTS "idx_xref_multi_target" ON "fed_data"."entity_xref_multi" ("target_dataset", "target_id");
-- Create "eo_bmf" table
CREATE TABLE IF NOT EXISTS "fed_data"."eo_bmf" (
  "ein" text NOT NULL,
  "name" text NOT NULL,
  "ico" text NULL,
  "street" text NULL,
  "city" text NULL,
  "state" text NULL,
  "zip" text NULL,
  "group_exemption" text NULL,
  "subsection" smallint NULL,
  "affiliation" smallint NULL,
  "classification" text NULL,
  "ruling" text NULL,
  "deductibility" smallint NULL,
  "foundation" smallint NULL,
  "activity" text NULL,
  "organization" smallint NULL,
  "status" smallint NULL,
  "tax_period" text NULL,
  "asset_cd" smallint NULL,
  "income_cd" smallint NULL,
  "filing_req_cd" smallint NULL,
  "pf_filing_req_cd" smallint NULL,
  "acct_pd" smallint NULL,
  "asset_amt" bigint NULL,
  "income_amt" bigint NULL,
  "revenue_amt" bigint NULL,
  "ntee_cd" text NULL,
  "sort_name" text NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("ein")
);
-- Create index "idx_eo_bmf_asset_amt" to table: "eo_bmf"
CREATE INDEX IF NOT EXISTS "idx_eo_bmf_asset_amt" ON "fed_data"."eo_bmf" ("asset_amt" DESC NULLS LAST);
-- Create index "idx_eo_bmf_name_trgm" to table: "eo_bmf"
CREATE INDEX IF NOT EXISTS "idx_eo_bmf_name_trgm" ON "fed_data"."eo_bmf" USING GIN ("name" public.gin_trgm_ops);
-- Create index "idx_eo_bmf_ntee" to table: "eo_bmf"
CREATE INDEX IF NOT EXISTS "idx_eo_bmf_ntee" ON "fed_data"."eo_bmf" ("ntee_cd");
-- Create index "idx_eo_bmf_state" to table: "eo_bmf"
CREATE INDEX IF NOT EXISTS "idx_eo_bmf_state" ON "fed_data"."eo_bmf" ("state");
-- Create index "idx_eo_bmf_subsection" to table: "eo_bmf"
CREATE INDEX IF NOT EXISTS "idx_eo_bmf_subsection" ON "fed_data"."eo_bmf" ("subsection");
-- Create "epa_facilities" table
CREATE TABLE IF NOT EXISTS "fed_data"."epa_facilities" (
  "registry_id" character varying(50) NOT NULL,
  "fac_name" character varying(300) NULL,
  "fac_city" character varying(100) NULL,
  "fac_state" character varying(10) NULL,
  "fac_zip" character varying(20) NULL,
  "naics_codes" text[] NULL,
  "sic_codes" text[] NULL,
  "fac_lat" numeric(9,6) NULL,
  "fac_long" numeric(9,6) NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("registry_id")
);
-- Create index "idx_epa_name" to table: "epa_facilities"
CREATE INDEX IF NOT EXISTS "idx_epa_name" ON "fed_data"."epa_facilities" USING GIN ("fac_name" public.gin_trgm_ops);
-- Create index "idx_epa_state" to table: "epa_facilities"
CREATE INDEX IF NOT EXISTS "idx_epa_state" ON "fed_data"."epa_facilities" ("fac_state");
-- Create "f13_filers" table
CREATE TABLE IF NOT EXISTS "fed_data"."f13_filers" (
  "cik" character varying(10) NOT NULL,
  "company_name" character varying(200) NOT NULL,
  "form_type" character varying(10) NULL,
  "filing_date" date NULL,
  "period_of_report" date NULL,
  "total_value" bigint NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("cik")
);
-- Create index "idx_f13_filers_name" to table: "f13_filers"
CREATE INDEX IF NOT EXISTS "idx_f13_filers_name" ON "fed_data"."f13_filers" USING GIN ("company_name" public.gin_trgm_ops);
-- Create "f13_holdings" table
CREATE TABLE IF NOT EXISTS "fed_data"."f13_holdings" (
  "cik" character varying(10) NOT NULL,
  "period" date NOT NULL,
  "cusip" character(9) NOT NULL,
  "issuer_name" character varying(200) NULL,
  "class_title" character varying(100) NULL,
  "value" bigint NULL,
  "shares" bigint NULL,
  "sh_prn_type" character varying(5) NULL,
  "put_call" character varying(4) NULL,
  PRIMARY KEY ("cik", "period", "cusip")
);
-- Create index "idx_f13_holdings_cusip" to table: "f13_holdings"
CREATE INDEX IF NOT EXISTS "idx_f13_holdings_cusip" ON "fed_data"."f13_holdings" ("cusip");
-- Create "sync_log" table
CREATE TABLE IF NOT EXISTS "fed_data"."sync_log" (
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
CREATE INDEX IF NOT EXISTS "idx_sync_log_dataset" ON "fed_data"."sync_log" ("dataset");
-- Create index "idx_sync_log_started" to table: "sync_log"
CREATE INDEX IF NOT EXISTS "idx_sync_log_started" ON "fed_data"."sync_log" ("started_at" DESC);
-- Create index "idx_sync_log_status" to table: "sync_log"
CREATE INDEX IF NOT EXISTS "idx_sync_log_status" ON "fed_data"."sync_log" ("status");
-- Create "abs_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."abs_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "firmpdemp" integer NULL,
  "rcppdemp" bigint NULL,
  "payann" bigint NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);
-- Create "susb_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."susb_data" (
  "year" smallint NOT NULL,
  "fips_state" character(2) NOT NULL,
  "naics" character varying(6) NOT NULL,
  "entrsizedscr" character varying(60) NOT NULL,
  "firm" integer NULL,
  "estb" integer NULL,
  "empl" integer NULL,
  "payr" bigint NULL,
  PRIMARY KEY ("year", "fips_state", "naics", "entrsizedscr")
);
-- Create index "idx_susb_naics" to table: "susb_data"
CREATE INDEX IF NOT EXISTS "idx_susb_naics" ON "fed_data"."susb_data" ("naics");
-- Create "adv_crs_enrichment" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_crs_enrichment" (
  "crd_number" integer NOT NULL,
  "crs_id" character varying(50) NOT NULL,
  "firm_type" character varying(100) NULL,
  "key_services" text NULL,
  "fee_types" jsonb NULL,
  "has_disciplinary_history" boolean NULL,
  "conflicts_of_interest" text NULL,
  "model" character varying(50) NULL,
  "input_tokens" integer NULL,
  "output_tokens" integer NULL,
  "enriched_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "crs_id")
);
-- Create index "idx_crs_enrich_firm_type" to table: "adv_crs_enrichment"
CREATE INDEX IF NOT EXISTS "idx_crs_enrich_firm_type" ON "fed_data"."adv_crs_enrichment" ("firm_type");
-- Create "form_5500" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_5500" (
  "ack_id" text NOT NULL,
  "form_plan_year_begin_date" text NULL,
  "form_tax_prd" text NULL,
  "type_plan_entity_cd" text NULL,
  "type_dfe_plan_entity_cd" text NULL,
  "initial_filing_ind" text NULL,
  "amended_ind" text NULL,
  "final_filing_ind" text NULL,
  "short_plan_yr_ind" text NULL,
  "collective_bargain_ind" text NULL,
  "f5558_application_filed_ind" text NULL,
  "ext_automatic_ind" text NULL,
  "dfvc_program_ind" text NULL,
  "ext_special_ind" text NULL,
  "ext_special_text" text NULL,
  "plan_name" text NULL,
  "spons_dfe_pn" text NULL,
  "plan_eff_date" text NULL,
  "sponsor_dfe_name" text NULL,
  "spons_dfe_dba_name" text NULL,
  "spons_dfe_care_of_name" text NULL,
  "spons_dfe_mail_us_address1" text NULL,
  "spons_dfe_mail_us_address2" text NULL,
  "spons_dfe_mail_us_city" text NULL,
  "spons_dfe_mail_us_state" text NULL,
  "spons_dfe_mail_us_zip" text NULL,
  "spons_dfe_mail_foreign_addr1" text NULL,
  "spons_dfe_mail_foreign_addr2" text NULL,
  "spons_dfe_mail_foreign_city" text NULL,
  "spons_dfe_mail_forgn_prov_st" text NULL,
  "spons_dfe_mail_foreign_cntry" text NULL,
  "spons_dfe_mail_forgn_postal_cd" text NULL,
  "spons_dfe_loc_us_address1" text NULL,
  "spons_dfe_loc_us_address2" text NULL,
  "spons_dfe_loc_us_city" text NULL,
  "spons_dfe_loc_us_state" text NULL,
  "spons_dfe_loc_us_zip" text NULL,
  "spons_dfe_loc_foreign_address1" text NULL,
  "spons_dfe_loc_foreign_address2" text NULL,
  "spons_dfe_loc_foreign_city" text NULL,
  "spons_dfe_loc_forgn_prov_st" text NULL,
  "spons_dfe_loc_foreign_cntry" text NULL,
  "spons_dfe_loc_forgn_postal_cd" text NULL,
  "spons_dfe_ein" text NULL,
  "spons_dfe_phone_num" text NULL,
  "business_code" text NULL,
  "admin_name" text NULL,
  "admin_care_of_name" text NULL,
  "admin_us_address1" text NULL,
  "admin_us_address2" text NULL,
  "admin_us_city" text NULL,
  "admin_us_state" text NULL,
  "admin_us_zip" text NULL,
  "admin_foreign_address1" text NULL,
  "admin_foreign_address2" text NULL,
  "admin_foreign_city" text NULL,
  "admin_foreign_prov_state" text NULL,
  "admin_foreign_cntry" text NULL,
  "admin_foreign_postal_cd" text NULL,
  "admin_ein" text NULL,
  "admin_phone_num" text NULL,
  "last_rpt_spons_name" text NULL,
  "last_rpt_spons_ein" text NULL,
  "last_rpt_plan_num" text NULL,
  "admin_signed_date" text NULL,
  "admin_signed_name" text NULL,
  "spons_signed_date" text NULL,
  "spons_signed_name" text NULL,
  "dfe_signed_date" text NULL,
  "dfe_signed_name" text NULL,
  "tot_partcp_boy_cnt" integer NULL,
  "tot_active_partcp_cnt" integer NULL,
  "rtd_sep_partcp_rcvg_cnt" integer NULL,
  "rtd_sep_partcp_fut_cnt" integer NULL,
  "subtl_act_rtd_sep_cnt" integer NULL,
  "benef_rcvg_bnft_cnt" integer NULL,
  "tot_act_rtd_sep_benef_cnt" integer NULL,
  "partcp_account_bal_cnt" integer NULL,
  "sep_partcp_partl_vstd_cnt" integer NULL,
  "contrib_emplrs_cnt" integer NULL,
  "type_pension_bnft_code" text NULL,
  "type_welfare_bnft_code" text NULL,
  "funding_insurance_ind" text NULL,
  "funding_sec412_ind" text NULL,
  "funding_trust_ind" text NULL,
  "funding_gen_asset_ind" text NULL,
  "benefit_insurance_ind" text NULL,
  "benefit_sec412_ind" text NULL,
  "benefit_trust_ind" text NULL,
  "benefit_gen_asset_ind" text NULL,
  "sch_r_attached_ind" text NULL,
  "sch_mb_attached_ind" text NULL,
  "sch_sb_attached_ind" text NULL,
  "sch_h_attached_ind" text NULL,
  "sch_i_attached_ind" text NULL,
  "sch_a_attached_ind" text NULL,
  "num_sch_a_attached_cnt" integer NULL,
  "sch_c_attached_ind" text NULL,
  "sch_d_attached_ind" text NULL,
  "sch_g_attached_ind" text NULL,
  "filing_status" text NULL,
  "date_received" text NULL,
  "valid_admin_signature" text NULL,
  "valid_dfe_signature" text NULL,
  "valid_sponsor_signature" text NULL,
  "admin_phone_num_foreign" text NULL,
  "spons_dfe_phone_num_foreign" text NULL,
  "admin_name_same_spon_ind" text NULL,
  "admin_address_same_spon_ind" text NULL,
  "preparer_name" text NULL,
  "preparer_firm_name" text NULL,
  "preparer_us_address1" text NULL,
  "preparer_us_address2" text NULL,
  "preparer_us_city" text NULL,
  "preparer_us_state" text NULL,
  "preparer_us_zip" text NULL,
  "preparer_foreign_address1" text NULL,
  "preparer_foreign_address2" text NULL,
  "preparer_foreign_city" text NULL,
  "preparer_foreign_prov_state" text NULL,
  "preparer_foreign_cntry" text NULL,
  "preparer_foreign_postal_cd" text NULL,
  "preparer_phone_num" text NULL,
  "preparer_phone_num_foreign" text NULL,
  "tot_act_partcp_boy_cnt" integer NULL,
  "subj_m1_filing_req_ind" text NULL,
  "compliance_m1_filing_req_ind" text NULL,
  "m1_receipt_confirmation_code" text NULL,
  "admin_manual_signed_date" text NULL,
  "admin_manual_signed_name" text NULL,
  "last_rpt_plan_name" text NULL,
  "spons_manual_signed_date" text NULL,
  "spons_manual_signed_name" text NULL,
  "dfe_manual_signed_date" text NULL,
  "dfe_manual_signed_name" text NULL,
  "adopted_plan_perm_sec_act" text NULL,
  "partcp_account_bal_cnt_boy" integer NULL,
  "sch_dcg_attached_ind" text NULL,
  "num_sch_dcg_attached_cnt" integer NULL,
  "sch_mep_attached_ind" text NULL,
  PRIMARY KEY ("ack_id")
);
-- Create index "idx_f5500_ein" to table: "form_5500"
CREATE INDEX IF NOT EXISTS "idx_f5500_ein" ON "fed_data"."form_5500" ("spons_dfe_ein");
-- Create index "idx_f5500_plan_year" to table: "form_5500"
CREATE INDEX IF NOT EXISTS "idx_f5500_plan_year" ON "fed_data"."form_5500" ("form_plan_year_begin_date");
-- Create index "idx_f5500_sponsor_trgm" to table: "form_5500"
CREATE INDEX IF NOT EXISTS "idx_f5500_sponsor_trgm" ON "fed_data"."form_5500" USING GIN ("sponsor_dfe_name" public.gin_trgm_ops);
-- Create index "idx_f5500_sponsor_upper" to table: "form_5500"
CREATE INDEX IF NOT EXISTS "idx_f5500_sponsor_upper" ON "fed_data"."form_5500" ((upper(TRIM(BOTH FROM sponsor_dfe_name))));
-- Create "form_5500_providers" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_5500_providers" (
  "ack_id" text NOT NULL,
  "row_order" integer NOT NULL,
  "provider_eligible_name" text NULL,
  "provider_eligible_ein" text NULL,
  "provider_eligible_us_address1" text NULL,
  "provider_eligible_us_address2" text NULL,
  "provider_eligible_us_city" text NULL,
  "provider_eligible_us_state" text NULL,
  "provider_eligible_us_zip" text NULL,
  "prov_eligible_foreign_address1" text NULL,
  "prov_eligible_foreign_address2" text NULL,
  "prov_eligible_foreign_city" text NULL,
  "prov_eligible_foreign_prov_st" text NULL,
  "prov_eligible_foreign_cntry" text NULL,
  "prov_eligible_foreign_post_cd" text NULL,
  PRIMARY KEY ("ack_id", "row_order")
);
-- Create index "idx_f5500prov_ein" to table: "form_5500_providers"
CREATE INDEX IF NOT EXISTS "idx_f5500prov_ein" ON "fed_data"."form_5500_providers" ("provider_eligible_ein");
-- Create index "idx_f5500prov_name_trgm" to table: "form_5500_providers"
CREATE INDEX IF NOT EXISTS "idx_f5500prov_name_trgm" ON "fed_data"."form_5500_providers" USING GIN ("provider_eligible_name" public.gin_trgm_ops);
-- Create "form_5500_schedule_h" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_5500_schedule_h" (
  "ack_id" text NOT NULL,
  "sch_h_plan_year_begin_date" text NULL,
  "sch_h_tax_prd" text NULL,
  "sch_h_pn" text NULL,
  "sch_h_ein" text NULL,
  "non_int_bear_cash_boy_amt" numeric NULL,
  "emplr_contrib_boy_amt" numeric NULL,
  "partcp_contrib_boy_amt" numeric NULL,
  "other_receivables_boy_amt" numeric NULL,
  "int_bear_cash_boy_amt" numeric NULL,
  "govt_sec_boy_amt" numeric NULL,
  "corp_debt_preferred_boy_amt" numeric NULL,
  "corp_debt_other_boy_amt" numeric NULL,
  "pref_stock_boy_amt" numeric NULL,
  "common_stock_boy_amt" numeric NULL,
  "joint_venture_boy_amt" numeric NULL,
  "real_estate_boy_amt" numeric NULL,
  "other_loans_boy_amt" numeric NULL,
  "partcp_loans_boy_amt" numeric NULL,
  "int_common_tr_boy_amt" numeric NULL,
  "int_pool_sep_acct_boy_amt" numeric NULL,
  "int_master_tr_boy_amt" numeric NULL,
  "int_103_12_invst_boy_amt" numeric NULL,
  "int_reg_invst_co_boy_amt" numeric NULL,
  "ins_co_gen_acct_boy_amt" numeric NULL,
  "oth_invst_boy_amt" numeric NULL,
  "emplr_sec_boy_amt" numeric NULL,
  "emplr_prop_boy_amt" numeric NULL,
  "bldgs_used_boy_amt" numeric NULL,
  "tot_assets_boy_amt" numeric NULL,
  "bnfts_payable_boy_amt" numeric NULL,
  "oprtng_payable_boy_amt" numeric NULL,
  "acquis_indbt_boy_amt" numeric NULL,
  "other_liab_boy_amt" numeric NULL,
  "tot_liabilities_boy_amt" numeric NULL,
  "net_assets_boy_amt" numeric NULL,
  "non_int_bear_cash_eoy_amt" numeric NULL,
  "emplr_contrib_eoy_amt" numeric NULL,
  "partcp_contrib_eoy_amt" numeric NULL,
  "other_receivables_eoy_amt" numeric NULL,
  "int_bear_cash_eoy_amt" numeric NULL,
  "govt_sec_eoy_amt" numeric NULL,
  "corp_debt_preferred_eoy_amt" numeric NULL,
  "corp_debt_other_eoy_amt" numeric NULL,
  "pref_stock_eoy_amt" numeric NULL,
  "common_stock_eoy_amt" numeric NULL,
  "joint_venture_eoy_amt" numeric NULL,
  "real_estate_eoy_amt" numeric NULL,
  "other_loans_eoy_amt" numeric NULL,
  "partcp_loans_eoy_amt" numeric NULL,
  "int_common_tr_eoy_amt" numeric NULL,
  "int_pool_sep_acct_eoy_amt" numeric NULL,
  "int_master_tr_eoy_amt" numeric NULL,
  "int_103_12_invst_eoy_amt" numeric NULL,
  "int_reg_invst_co_eoy_amt" numeric NULL,
  "ins_co_gen_acct_eoy_amt" numeric NULL,
  "oth_invst_eoy_amt" numeric NULL,
  "emplr_sec_eoy_amt" numeric NULL,
  "emplr_prop_eoy_amt" numeric NULL,
  "bldgs_used_eoy_amt" numeric NULL,
  "tot_assets_eoy_amt" numeric NULL,
  "bnfts_payable_eoy_amt" numeric NULL,
  "oprtng_payable_eoy_amt" numeric NULL,
  "acquis_indbt_eoy_amt" numeric NULL,
  "other_liab_eoy_amt" numeric NULL,
  "tot_liabilities_eoy_amt" numeric NULL,
  "net_assets_eoy_amt" numeric NULL,
  "emplr_contrib_income_amt" numeric NULL,
  "participant_contrib_amt" numeric NULL,
  "oth_contrib_rcvd_amt" numeric NULL,
  "non_cash_contrib_bs_amt" numeric NULL,
  "tot_contrib_amt" numeric NULL,
  "int_bear_cash_amt" numeric NULL,
  "int_on_govt_sec_amt" numeric NULL,
  "int_on_corp_debt_amt" numeric NULL,
  "int_on_oth_loans_amt" numeric NULL,
  "int_on_partcp_loans_amt" numeric NULL,
  "int_on_oth_invst_amt" numeric NULL,
  "total_interest_amt" numeric NULL,
  "divnd_pref_stock_amt" numeric NULL,
  "divnd_common_stock_amt" numeric NULL,
  "registered_invst_amt" numeric NULL,
  "total_dividends_amt" numeric NULL,
  "total_rents_amt" numeric NULL,
  "aggregate_proceeds_amt" numeric NULL,
  "aggregate_costs_amt" numeric NULL,
  "tot_gain_loss_sale_ast_amt" numeric NULL,
  "unrealzd_apprctn_re_amt" numeric NULL,
  "unrealzd_apprctn_oth_amt" numeric NULL,
  "tot_unrealzd_apprctn_amt" numeric NULL,
  "gain_loss_com_trust_amt" numeric NULL,
  "gain_loss_pool_sep_amt" numeric NULL,
  "gain_loss_master_tr_amt" numeric NULL,
  "gain_loss_103_12_invst_amt" numeric NULL,
  "gain_loss_reg_invst_amt" numeric NULL,
  "other_income_amt" numeric NULL,
  "tot_income_amt" numeric NULL,
  "distrib_drt_partcp_amt" numeric NULL,
  "ins_carrier_bnfts_amt" numeric NULL,
  "oth_bnft_payment_amt" numeric NULL,
  "tot_distrib_bnft_amt" numeric NULL,
  "tot_corrective_distrib_amt" numeric NULL,
  "tot_deemed_distr_part_lns_amt" numeric NULL,
  "tot_int_expense_amt" numeric NULL,
  "professional_fees_amt" numeric NULL,
  "contract_admin_fees_amt" numeric NULL,
  "invst_mgmt_fees_amt" numeric NULL,
  "other_admin_fees_amt" numeric NULL,
  "tot_admin_expenses_amt" numeric NULL,
  "tot_expenses_amt" numeric NULL,
  "net_income_amt" numeric NULL,
  "tot_transfers_to_amt" numeric NULL,
  "tot_transfers_from_amt" numeric NULL,
  "acctnt_opinion_type_cd" text NULL,
  "acct_performed_ltd_audit_ind" text NULL,
  "accountant_firm_name" text NULL,
  "accountant_firm_ein" text NULL,
  "acct_opin_not_on_file_ind" text NULL,
  "fail_transmit_contrib_ind" text NULL,
  "fail_transmit_contrib_amt" numeric NULL,
  "loans_in_default_ind" text NULL,
  "loans_in_default_amt" numeric NULL,
  "leases_in_default_ind" text NULL,
  "leases_in_default_amt" numeric NULL,
  "party_in_int_not_rptd_ind" text NULL,
  "party_in_int_not_rptd_amt" numeric NULL,
  "plan_ins_fdlty_bond_ind" text NULL,
  "plan_ins_fdlty_bond_amt" numeric NULL,
  "loss_discv_dur_year_ind" text NULL,
  "loss_discv_dur_year_amt" numeric NULL,
  "asset_undeterm_val_ind" text NULL,
  "asset_undeterm_val_amt" numeric NULL,
  "non_cash_contrib_ind" text NULL,
  "non_cash_contrib_amt" numeric NULL,
  "ast_held_invst_ind" text NULL,
  "five_prcnt_trans_ind" text NULL,
  "all_plan_ast_distrib_ind" text NULL,
  "fail_provide_benefit_due_ind" text NULL,
  "fail_provide_benefit_due_amt" numeric NULL,
  "plan_blackout_period_ind" text NULL,
  "comply_blackout_notice_ind" text NULL,
  "res_term_plan_adpt_ind" text NULL,
  "res_term_plan_adpt_amt" numeric NULL,
  "fdcry_trust_ein" text NULL,
  "fdcry_trust_name" text NULL,
  "covered_pbgc_insurance_ind" text NULL,
  "trust_incur_unrel_tax_inc_ind" text NULL,
  "trust_incur_unrel_tax_inc_amt" numeric NULL,
  "in_service_distrib_ind" text NULL,
  "in_service_distrib_amt" numeric NULL,
  "fdcry_trustee_cust_name" text NULL,
  "fdcry_trust_cust_phon_num" text NULL,
  "fdcry_trust_cust_phon_nu_fore" text NULL,
  "distrib_made_employee_62_ind" text NULL,
  "premium_filing_confirm_number" text NULL,
  "acct_perf_ltd_audit_103_8_ind" text NULL,
  "acct_perf_ltd_audit_103_12_ind" text NULL,
  "acct_perf_not_ltd_audit_ind" text NULL,
  "salaries_allowances_amt" numeric NULL,
  "oth_recordkeeping_fees_amt" numeric NULL,
  "iqpa_audit_fees_amt" numeric NULL,
  "trustee_custodial_fees_amt" numeric NULL,
  "actuarial_fees_amt" numeric NULL,
  "legal_fees_amt" numeric NULL,
  "valuation_appraisal_fees_amt" numeric NULL,
  "other_trustee_fees_expenses_amt" numeric NULL,
  PRIMARY KEY ("ack_id")
);
-- Create index "idx_f5500sch_h_ein" to table: "form_5500_schedule_h"
CREATE INDEX IF NOT EXISTS "idx_f5500sch_h_ein" ON "fed_data"."form_5500_schedule_h" ("sch_h_ein");
-- Create index "idx_f5500sch_h_tot_assets" to table: "form_5500_schedule_h"
CREATE INDEX IF NOT EXISTS "idx_f5500sch_h_tot_assets" ON "fed_data"."form_5500_schedule_h" ("tot_assets_eoy_amt");
-- Create "form_5500_sf" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_5500_sf" (
  "ack_id" text NOT NULL,
  "sf_plan_year_begin_date" text NULL,
  "sf_tax_prd" text NULL,
  "sf_plan_entity_cd" text NULL,
  "sf_initial_filing_ind" text NULL,
  "sf_amended_ind" text NULL,
  "sf_final_filing_ind" text NULL,
  "sf_short_plan_yr_ind" text NULL,
  "sf_5558_application_filed_ind" text NULL,
  "sf_ext_automatic_ind" text NULL,
  "sf_dfvc_program_ind" text NULL,
  "sf_ext_special_ind" text NULL,
  "sf_ext_special_text" text NULL,
  "sf_plan_name" text NULL,
  "sf_plan_num" text NULL,
  "sf_plan_eff_date" text NULL,
  "sf_sponsor_name" text NULL,
  "sf_sponsor_dfe_dba_name" text NULL,
  "sf_spons_us_address1" text NULL,
  "sf_spons_us_address2" text NULL,
  "sf_spons_us_city" text NULL,
  "sf_spons_us_state" text NULL,
  "sf_spons_us_zip" text NULL,
  "sf_spons_foreign_address1" text NULL,
  "sf_spons_foreign_address2" text NULL,
  "sf_spons_foreign_city" text NULL,
  "sf_spons_foreign_prov_state" text NULL,
  "sf_spons_foreign_cntry" text NULL,
  "sf_spons_foreign_postal_cd" text NULL,
  "sf_spons_ein" text NULL,
  "sf_spons_phone_num" text NULL,
  "sf_business_code" text NULL,
  "sf_admin_name" text NULL,
  "sf_admin_care_of_name" text NULL,
  "sf_admin_us_address1" text NULL,
  "sf_admin_us_address2" text NULL,
  "sf_admin_us_city" text NULL,
  "sf_admin_us_state" text NULL,
  "sf_admin_us_zip" text NULL,
  "sf_admin_foreign_address1" text NULL,
  "sf_admin_foreign_address2" text NULL,
  "sf_admin_foreign_city" text NULL,
  "sf_admin_foreign_prov_state" text NULL,
  "sf_admin_foreign_cntry" text NULL,
  "sf_admin_foreign_postal_cd" text NULL,
  "sf_admin_ein" text NULL,
  "sf_admin_phone_num" text NULL,
  "sf_last_rpt_spons_name" text NULL,
  "sf_last_rpt_spons_ein" text NULL,
  "sf_last_rpt_plan_num" text NULL,
  "sf_tot_partcp_boy_cnt" integer NULL,
  "sf_tot_act_rtd_sep_benef_cnt" integer NULL,
  "sf_partcp_account_bal_cnt" integer NULL,
  "sf_eligible_assets_ind" text NULL,
  "sf_iqpa_waiver_ind" text NULL,
  "sf_tot_assets_boy_amt" numeric NULL,
  "sf_tot_liabilities_boy_amt" numeric NULL,
  "sf_net_assets_boy_amt" numeric NULL,
  "sf_tot_assets_eoy_amt" numeric NULL,
  "sf_tot_liabilities_eoy_amt" numeric NULL,
  "sf_net_assets_eoy_amt" numeric NULL,
  "sf_emplr_contrib_income_amt" numeric NULL,
  "sf_particip_contrib_income_amt" numeric NULL,
  "sf_oth_contrib_rcvd_amt" numeric NULL,
  "sf_other_income_amt" numeric NULL,
  "sf_tot_income_amt" numeric NULL,
  "sf_tot_distrib_bnft_amt" numeric NULL,
  "sf_corrective_deemed_distr_amt" numeric NULL,
  "sf_admin_srvc_providers_amt" numeric NULL,
  "sf_oth_expenses_amt" numeric NULL,
  "sf_tot_expenses_amt" numeric NULL,
  "sf_net_income_amt" numeric NULL,
  "sf_tot_plan_transfers_amt" numeric NULL,
  "sf_type_pension_bnft_code" text NULL,
  "sf_type_welfare_bnft_code" text NULL,
  "sf_fail_transmit_contrib_ind" text NULL,
  "sf_fail_transmit_contrib_amt" numeric NULL,
  "sf_party_in_int_not_rptd_ind" text NULL,
  "sf_party_in_int_not_rptd_amt" numeric NULL,
  "sf_plan_ins_fdlty_bond_ind" text NULL,
  "sf_plan_ins_fdlty_bond_amt" numeric NULL,
  "sf_loss_discv_dur_year_ind" text NULL,
  "sf_loss_discv_dur_year_amt" numeric NULL,
  "sf_broker_fees_paid_ind" text NULL,
  "sf_broker_fees_paid_amt" numeric NULL,
  "sf_fail_provide_benef_due_ind" text NULL,
  "sf_fail_provide_benef_due_amt" numeric NULL,
  "sf_partcp_loans_ind" text NULL,
  "sf_partcp_loans_eoy_amt" numeric NULL,
  "sf_plan_blackout_period_ind" text NULL,
  "sf_comply_blackout_notice_ind" text NULL,
  "sf_db_plan_funding_reqd_ind" text NULL,
  "sf_dc_plan_funding_reqd_ind" text NULL,
  "sf_ruling_letter_grant_date" text NULL,
  "sf_sec_412_req_contrib_amt" numeric NULL,
  "sf_emplr_contrib_paid_amt" numeric NULL,
  "sf_funding_deficiency_amt" numeric NULL,
  "sf_funding_deadline_ind" text NULL,
  "sf_res_term_plan_adpt_ind" text NULL,
  "sf_res_term_plan_adpt_amt" numeric NULL,
  "sf_all_plan_ast_distrib_ind" text NULL,
  "sf_admin_signed_date" text NULL,
  "sf_admin_signed_name" text NULL,
  "sf_spons_signed_date" text NULL,
  "sf_spons_signed_name" text NULL,
  "filing_status" text NULL,
  "date_received" text NULL,
  "valid_admin_signature" text NULL,
  "valid_sponsor_signature" text NULL,
  "sf_admin_phone_num_foreign" text NULL,
  "sf_spons_care_of_name" text NULL,
  "sf_spons_loc_foreign_address1" text NULL,
  "sf_spons_loc_foreign_address2" text NULL,
  "sf_spons_loc_foreign_city" text NULL,
  "sf_spons_loc_foreign_cntry" text NULL,
  "sf_spons_loc_foreign_postal_cd" text NULL,
  "sf_spons_loc_foreign_prov_stat" text NULL,
  "sf_spons_loc_us_address1" text NULL,
  "sf_spons_loc_us_address2" text NULL,
  "sf_spons_loc_us_city" text NULL,
  "sf_spons_loc_us_state" text NULL,
  "sf_spons_loc_us_zip" text NULL,
  "sf_spons_phone_num_foreign" text NULL,
  "sf_admin_name_same_spon_ind" text NULL,
  "sf_admin_addrss_same_spon_ind" text NULL,
  "sf_preparer_name" text NULL,
  "sf_preparer_firm_name" text NULL,
  "sf_preparer_us_address1" text NULL,
  "sf_preparer_us_address2" text NULL,
  "sf_preparer_us_city" text NULL,
  "sf_preparer_us_state" text NULL,
  "sf_preparer_us_zip" text NULL,
  "sf_preparer_foreign_address1" text NULL,
  "sf_preparer_foreign_address2" text NULL,
  "sf_preparer_foreign_city" text NULL,
  "sf_preparer_foreign_prov_state" text NULL,
  "sf_preparer_foreign_cntry" text NULL,
  "sf_preparer_foreign_postal_cd" text NULL,
  "sf_preparer_phone_num" text NULL,
  "sf_preparer_phone_num_foreign" text NULL,
  "sf_fdcry_trust_name" text NULL,
  "sf_fdcry_trust_ein" text NULL,
  "sf_unp_min_cont_cur_yrtot_amt" numeric NULL,
  "sf_covered_pbgc_insurance_ind" text NULL,
  "sf_tot_act_partcp_boy_cnt" integer NULL,
  "sf_tot_act_partcp_eoy_cnt" integer NULL,
  "sf_sep_partcp_partl_vstd_cnt" integer NULL,
  "sf_trus_inc_unrel_tax_inc_ind" text NULL,
  "sf_trus_inc_unrel_tax_inc_amt" numeric NULL,
  "sf_fdcry_truste_cust_name" text NULL,
  "sf_fdcry_truste_cust_phone_num" text NULL,
  "sf_fdcry_trus_cus_phon_numfore" text NULL,
  "sf_401k_plan_ind" text NULL,
  "sf_401k_satisfy_rqmts_ind" text NULL,
  "sf_adp_acp_test_ind" text NULL,
  "sf_mthd_used_satisfy_rqmts_ind" text NULL,
  "sf_plan_satisfy_tests_ind" text NULL,
  "sf_plan_timely_amended_ind" text NULL,
  "sf_last_plan_amendment_date" text NULL,
  "sf_tax_code" text NULL,
  "sf_last_opin_advi_date" text NULL,
  "sf_last_opin_advi_serial_num" text NULL,
  "sf_fav_determ_ltr_date" text NULL,
  "sf_plan_maintain_us_terri_ind" text NULL,
  "sf_in_service_distrib_ind" text NULL,
  "sf_in_service_distrib_amt" numeric NULL,
  "sf_min_req_distrib_ind" text NULL,
  "sf_admin_manual_sign_date" text NULL,
  "sf_admin_manual_signed_name" text NULL,
  "sf_401k_design_based_safe_ind" text NULL,
  "sf_401k_prior_year_adp_ind" text NULL,
  "sf_401k_current_year_adp_ind" text NULL,
  "sf_401k_na_ind" text NULL,
  "sf_mthd_ratio_prcnt_test_ind" text NULL,
  "sf_mthd_avg_bnft_test_ind" text NULL,
  "sf_mthd_na_ind" text NULL,
  "sf_distrib_made_employe_62_ind" text NULL,
  "sf_last_rpt_plan_name" text NULL,
  "sf_premium_filing_confirm_no" text NULL,
  "sf_spons_manual_signed_date" text NULL,
  "sf_spons_manual_signed_name" text NULL,
  "sf_pbgc_notified_cd" text NULL,
  "sf_pbgc_notified_explan_text" text NULL,
  "sf_adopted_plan_perm_sec_act" text NULL,
  "collectively_bargained" text NULL,
  "sf_partcp_account_bal_cnt_boy" integer NULL,
  "sf_401k_design_based_safe_harbor_ind" text NULL,
  "sf_401k_prior_year_adp_test_ind" text NULL,
  "sf_401k_current_year_adp_test_ind" text NULL,
  "sf_opin_letter_date" text NULL,
  "sf_opin_letter_serial_num" text NULL,
  PRIMARY KEY ("ack_id")
);
-- Create index "idx_f5500sf_ein" to table: "form_5500_sf"
CREATE INDEX IF NOT EXISTS "idx_f5500sf_ein" ON "fed_data"."form_5500_sf" ("sf_spons_ein");
-- Create index "idx_f5500sf_plan_year" to table: "form_5500_sf"
CREATE INDEX IF NOT EXISTS "idx_f5500sf_plan_year" ON "fed_data"."form_5500_sf" ("sf_plan_year_begin_date");
-- Create index "idx_f5500sf_sponsor_trgm" to table: "form_5500_sf"
CREATE INDEX IF NOT EXISTS "idx_f5500sf_sponsor_trgm" ON "fed_data"."form_5500_sf" USING GIN ("sf_sponsor_name" public.gin_trgm_ops);
-- Create "form_bd" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_bd" (
  "crd_number" integer NOT NULL,
  "sec_number" character varying(20) NULL,
  "firm_name" character varying(300) NULL,
  "city" character varying(100) NULL,
  "state" character(2) NULL,
  "fiscal_year_end" character varying(4) NULL,
  "num_reps" integer NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number")
);
-- Create "form_d" table
CREATE TABLE IF NOT EXISTS "fed_data"."form_d" (
  "accession_number" character varying(25) NOT NULL,
  "cik" character varying(10) NULL,
  "entity_name" character varying(200) NULL,
  "entity_type" character varying(50) NULL,
  "year_of_inc" character varying(4) NULL,
  "state_of_inc" character(2) NULL,
  "industry_group" character varying(100) NULL,
  "revenue_range" character varying(50) NULL,
  "total_offering" bigint NULL,
  "total_sold" bigint NULL,
  "filing_date" date NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("accession_number")
);
-- Create index "idx_form_d_cik" to table: "form_d"
CREATE INDEX IF NOT EXISTS "idx_form_d_cik" ON "fed_data"."form_d" ("cik");
-- Create index "idx_form_d_name" to table: "form_d"
CREATE INDEX IF NOT EXISTS "idx_form_d_name" ON "fed_data"."form_d" USING GIN ("entity_name" public.gin_trgm_ops);
-- Create "fpds_contracts" table
CREATE TABLE IF NOT EXISTS "fed_data"."fpds_contracts" (
  "contract_id" character varying(50) NOT NULL,
  "piid" character varying(50) NULL,
  "agency_id" character varying(4) NULL,
  "agency_name" character varying(200) NULL,
  "vendor_name" character varying(200) NULL,
  "vendor_duns" character varying(13) NULL,
  "vendor_uei" character varying(12) NULL,
  "vendor_city" character varying(100) NULL,
  "vendor_state" character(2) NULL,
  "vendor_zip" character varying(10) NULL,
  "naics" character varying(6) NULL,
  "psc" character varying(4) NULL,
  "date_signed" date NULL,
  "dollars_obligated" numeric(15,2) NULL,
  "description" text NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("contract_id")
);
-- Create index "idx_fpds_date" to table: "fpds_contracts"
CREATE INDEX IF NOT EXISTS "idx_fpds_date" ON "fed_data"."fpds_contracts" ("date_signed");
-- Create index "idx_fpds_naics" to table: "fpds_contracts"
CREATE INDEX IF NOT EXISTS "idx_fpds_naics" ON "fed_data"."fpds_contracts" ("naics");
-- Create index "idx_fpds_vendor_name" to table: "fpds_contracts"
CREATE INDEX IF NOT EXISTS "idx_fpds_vendor_name" ON "fed_data"."fpds_contracts" USING GIN ("vendor_name" public.gin_trgm_ops);
-- Create index "idx_fpds_vendor_state" to table: "fpds_contracts"
CREATE INDEX IF NOT EXISTS "idx_fpds_vendor_state" ON "fed_data"."fpds_contracts" ("vendor_state");
-- Create index "idx_fpds_vendor_uei" to table: "fpds_contracts"
CREATE INDEX IF NOT EXISTS "idx_fpds_vendor_uei" ON "fed_data"."fpds_contracts" ("vendor_uei");
-- Create "fred_series" table
CREATE TABLE IF NOT EXISTS "fed_data"."fred_series" (
  "series_id" character varying(30) NOT NULL,
  "obs_date" date NOT NULL,
  "value" numeric NULL,
  PRIMARY KEY ("series_id", "obs_date")
);
-- Create index "idx_fred_date" to table: "fred_series"
CREATE INDEX IF NOT EXISTS "idx_fred_date" ON "fed_data"."fred_series" ("obs_date" DESC);
-- Create "laus_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."laus_data" (
  "series_id" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "period" character varying(3) NOT NULL,
  "value" numeric(12,1) NULL,
  PRIMARY KEY ("series_id", "year", "period")
);
-- Create "m3_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."m3_data" (
  "category" character varying(50) NOT NULL,
  "data_type" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "month" smallint NOT NULL,
  "value" bigint NULL,
  PRIMARY KEY ("category", "data_type", "year", "month")
);
-- Create "naics_codes" table
CREATE TABLE IF NOT EXISTS "fed_data"."naics_codes" (
  "code" character varying(6) NOT NULL,
  "title" character varying(300) NOT NULL,
  "sector" character(2) NOT NULL,
  "subsector" character(3) NULL,
  "industry_group" character(4) NULL,
  "description" text NULL,
  PRIMARY KEY ("code")
);
-- Create index "idx_naics_sector" to table: "naics_codes"
CREATE INDEX IF NOT EXISTS "idx_naics_sector" ON "fed_data"."naics_codes" ("sector");
-- Create "ncen_advisers" table
CREATE TABLE IF NOT EXISTS "fed_data"."ncen_advisers" (
  "fund_id" text NOT NULL,
  "adviser_name" text NOT NULL DEFAULT '',
  "adviser_crd" text NULL,
  "adviser_lei" text NULL,
  "file_num" text NULL,
  "adviser_type" text NOT NULL DEFAULT '',
  "state" text NULL,
  "country" text NULL,
  "is_affiliated" boolean NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("fund_id", "adviser_name", "adviser_type")
);
-- Create index "idx_ncen_advisers_crd" to table: "ncen_advisers"
CREATE INDEX IF NOT EXISTS "idx_ncen_advisers_crd" ON "fed_data"."ncen_advisers" ("adviser_crd");
-- Create index "idx_ncen_advisers_fund" to table: "ncen_advisers"
CREATE INDEX IF NOT EXISTS "idx_ncen_advisers_fund" ON "fed_data"."ncen_advisers" ("fund_id");
-- Create "ncen_funds" table
CREATE TABLE IF NOT EXISTS "fed_data"."ncen_funds" (
  "fund_id" text NOT NULL,
  "accession_number" text NOT NULL,
  "fund_name" text NULL,
  "series_id" text NULL,
  "lei" text NULL,
  "is_etf" boolean NULL,
  "is_index" boolean NULL,
  "is_money_market" boolean NULL,
  "is_target_date" boolean NULL,
  "is_fund_of_fund" boolean NULL,
  "monthly_avg_net_assets" numeric NULL,
  "daily_avg_net_assets" numeric NULL,
  "nav_per_share" numeric NULL,
  "management_fee" numeric NULL,
  "net_operating_expenses" numeric NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("fund_id")
);
-- Create index "idx_ncen_funds_accession" to table: "ncen_funds"
CREATE INDEX IF NOT EXISTS "idx_ncen_funds_accession" ON "fed_data"."ncen_funds" ("accession_number");
-- Create index "idx_ncen_funds_series" to table: "ncen_funds"
CREATE INDEX IF NOT EXISTS "idx_ncen_funds_series" ON "fed_data"."ncen_funds" ("series_id");
-- Create "ncen_registrants" table
CREATE TABLE IF NOT EXISTS "fed_data"."ncen_registrants" (
  "accession_number" text NOT NULL,
  "cik" text NOT NULL,
  "registrant_name" text NULL,
  "file_num" text NULL,
  "lei" text NULL,
  "address1" text NULL,
  "address2" text NULL,
  "city" text NULL,
  "state" text NULL,
  "country" text NULL,
  "zip" text NULL,
  "phone" text NULL,
  "investment_company_type" text NULL,
  "total_series" integer NULL,
  "filing_date" date NULL,
  "report_ending_period" date NULL,
  "is_first_filing" boolean NULL,
  "is_last_filing" boolean NULL,
  "family_investment_company_name" text NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("accession_number")
);
-- Create index "idx_ncen_registrants_cik" to table: "ncen_registrants"
CREATE INDEX IF NOT EXISTS "idx_ncen_registrants_cik" ON "fed_data"."ncen_registrants" ("cik");
-- Create "ncua_call_reports" table
CREATE TABLE IF NOT EXISTS "fed_data"."ncua_call_reports" (
  "cu_number" integer NOT NULL,
  "cycle_date" date NOT NULL,
  "cu_name" text NOT NULL,
  "street" text NULL,
  "city" text NULL,
  "state" text NULL,
  "zip_code" text NULL,
  "county" text NULL,
  "cu_type" smallint NULL,
  "region" smallint NULL,
  "peer_group" smallint NULL,
  "total_assets" bigint NULL,
  "total_loans" bigint NULL,
  "total_shares" bigint NULL,
  "total_borrowings" bigint NULL,
  "net_worth" bigint NULL,
  "net_income" bigint NULL,
  "gross_income" bigint NULL,
  "total_expenses" bigint NULL,
  "provision_losses" bigint NULL,
  "members" integer NULL,
  "net_worth_ratio" numeric(10,4) NULL,
  "member_business_loans" bigint NULL,
  "investments" bigint NULL,
  "num_employees" integer NULL,
  "synced_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("cu_number", "cycle_date")
);
-- Create index "idx_ncua_call_reports_cu_name_trgm" to table: "ncua_call_reports"
CREATE INDEX IF NOT EXISTS "idx_ncua_call_reports_cu_name_trgm" ON "fed_data"."ncua_call_reports" USING GIN ("cu_name" public.gin_trgm_ops);
-- Create index "idx_ncua_call_reports_cu_number" to table: "ncua_call_reports"
CREATE INDEX IF NOT EXISTS "idx_ncua_call_reports_cu_number" ON "fed_data"."ncua_call_reports" ("cu_number");
-- Create index "idx_ncua_call_reports_cycle_date" to table: "ncua_call_reports"
CREATE INDEX IF NOT EXISTS "idx_ncua_call_reports_cycle_date" ON "fed_data"."ncua_call_reports" ("cycle_date" DESC);
-- Create index "idx_ncua_call_reports_state" to table: "ncua_call_reports"
CREATE INDEX IF NOT EXISTS "idx_ncua_call_reports_state" ON "fed_data"."ncua_call_reports" ("state");
-- Create index "idx_ncua_call_reports_total_assets" to table: "ncua_call_reports"
CREATE INDEX IF NOT EXISTS "idx_ncua_call_reports_total_assets" ON "fed_data"."ncua_call_reports" ("total_assets" DESC);
-- Create "oews_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."oews_data" (
  "area_code" character varying(7) NOT NULL,
  "area_type" smallint NULL,
  "naics" character varying(6) NOT NULL,
  "occ_code" character varying(7) NOT NULL,
  "year" smallint NOT NULL,
  "tot_emp" integer NULL,
  "h_mean" numeric(10,2) NULL,
  "a_mean" integer NULL,
  "h_median" numeric(10,2) NULL,
  "a_median" integer NULL,
  PRIMARY KEY ("area_code", "naics", "occ_code", "year")
);
-- Create index "idx_oews_naics" to table: "oews_data"
CREATE INDEX IF NOT EXISTS "idx_oews_naics" ON "fed_data"."oews_data" ("naics");
-- Create index "idx_oews_occ" to table: "oews_data"
CREATE INDEX IF NOT EXISTS "idx_oews_occ" ON "fed_data"."oews_data" ("occ_code");
-- Create "osha_inspections" table
CREATE TABLE IF NOT EXISTS "fed_data"."osha_inspections" (
  "activity_nr" bigint NOT NULL,
  "estab_name" character varying(300) NULL,
  "site_city" character varying(100) NULL,
  "site_state" character(2) NULL,
  "site_zip" character varying(10) NULL,
  "naics_code" character varying(6) NULL,
  "sic_code" character varying(4) NULL,
  "open_date" date NULL,
  "close_case_date" date NULL,
  "case_type" character(1) NULL,
  "safety_hlth" character(1) NULL,
  "total_penalty" numeric(12,2) NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("activity_nr")
);
-- Create index "idx_osha_naics" to table: "osha_inspections"
CREATE INDEX IF NOT EXISTS "idx_osha_naics" ON "fed_data"."osha_inspections" ("naics_code");
-- Create index "idx_osha_name" to table: "osha_inspections"
CREATE INDEX IF NOT EXISTS "idx_osha_name" ON "fed_data"."osha_inspections" USING GIN ("estab_name" public.gin_trgm_ops);
-- Create "sic_crosswalk" table
CREATE TABLE IF NOT EXISTS "fed_data"."sic_crosswalk" (
  "sic_code" character(4) NOT NULL,
  "sic_description" character varying(200) NULL,
  "naics_code" character varying(6) NOT NULL,
  "naics_description" character varying(300) NULL,
  PRIMARY KEY ("sic_code", "naics_code")
);
-- Create index "idx_sic_xwalk_naics" to table: "sic_crosswalk"
CREATE INDEX IF NOT EXISTS "idx_sic_xwalk_naics" ON "fed_data"."sic_crosswalk" ("naics_code");
-- Create index "idx_sic_xwalk_sic" to table: "sic_crosswalk"
CREATE INDEX IF NOT EXISTS "idx_sic_xwalk_sic" ON "fed_data"."sic_crosswalk" ("sic_code");
-- Create "sec_enforcement_actions" table
CREATE TABLE IF NOT EXISTS "fed_data"."sec_enforcement_actions" (
  "action_id" character varying(50) NOT NULL,
  "action_type" character varying(50) NOT NULL,
  "respondent_name" character varying(300) NULL,
  "crd_number" integer NULL,
  "cik" character varying(20) NULL,
  "action_date" date NULL,
  "description" text NULL,
  "outcome" character varying(100) NULL,
  "penalty_amount" bigint NULL,
  "url" character varying(500) NULL,
  "synced_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("action_id")
);
-- Create index "idx_enforcement_crd" to table: "sec_enforcement_actions"
CREATE INDEX IF NOT EXISTS "idx_enforcement_crd" ON "fed_data"."sec_enforcement_actions" ("crd_number");
-- Create index "idx_enforcement_date" to table: "sec_enforcement_actions"
CREATE INDEX IF NOT EXISTS "idx_enforcement_date" ON "fed_data"."sec_enforcement_actions" ("action_date" DESC);
-- Create index "idx_enforcement_respondent" to table: "sec_enforcement_actions"
CREATE INDEX IF NOT EXISTS "idx_enforcement_respondent" ON "fed_data"."sec_enforcement_actions" ("respondent_name");
-- Create "schema_migrations" table
CREATE TABLE IF NOT EXISTS "fed_data"."schema_migrations" (
  "id" serial NOT NULL,
  "filename" text NOT NULL,
  "applied_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  CONSTRAINT "schema_migrations_filename_key" UNIQUE ("filename")
);
-- Create "sba_loans" table
CREATE TABLE IF NOT EXISTS "fed_data"."sba_loans" (
  "program" character varying(3) NOT NULL,
  "l2locid" bigint NOT NULL,
  "borrname" text NOT NULL,
  "borrstreet" text NULL,
  "borrcity" text NULL,
  "borrstate" text NULL,
  "borrzip" text NULL,
  "grossapproval" numeric NULL,
  "sbaguaranteedapproval" numeric NULL,
  "approvaldate" date NULL,
  "approvalfiscalyear" integer NULL,
  "firstdisbursementdate" date NULL,
  "terminmonths" integer NULL,
  "initialinterestrate" numeric NULL,
  "fixedorvariableinterestind" text NULL,
  "naicscode" text NULL,
  "naicsdescription" text NULL,
  "loanstatus" text NULL,
  "paidinfulldate" date NULL,
  "chargeoffdate" date NULL,
  "grosschargeoffamount" numeric NULL,
  "jobssupported" integer NULL,
  "businesstype" text NULL,
  "businessage" text NULL,
  "franchisecode" text NULL,
  "franchisename" text NULL,
  "processingmethod" text NULL,
  "subprogram" text NULL,
  "projectcounty" text NULL,
  "projectstate" text NULL,
  "sbadistrictoffice" text NULL,
  "congressionaldistrict" text NULL,
  "bankname" text NULL,
  "bankfdicnumber" text NULL,
  "bankncuanumber" text NULL,
  "bankstreet" text NULL,
  "bankcity" text NULL,
  "bankstate" text NULL,
  "bankzip" text NULL,
  "revolverstatus" integer NULL,
  "collateralind" text NULL,
  "soldsecmrktind" text NULL,
  "cdc_name" text NULL,
  "cdc_street" text NULL,
  "cdc_city" text NULL,
  "cdc_state" text NULL,
  "cdc_zip" text NULL,
  "thirdpartylender_name" text NULL,
  "thirdpartylender_city" text NULL,
  "thirdpartylender_state" text NULL,
  "thirdpartydollars" numeric NULL,
  "deliverymethod" text NULL,
  "synced_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("program", "l2locid")
);
-- Create index "idx_sba_loans_fdic" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_fdic" ON "fed_data"."sba_loans" ("bankfdicnumber") WHERE (bankfdicnumber IS NOT NULL);
-- Create index "idx_sba_loans_fy" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_fy" ON "fed_data"."sba_loans" ("approvalfiscalyear");
-- Create index "idx_sba_loans_naics" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_naics" ON "fed_data"."sba_loans" ("naicscode");
-- Create index "idx_sba_loans_name_trgm" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_name_trgm" ON "fed_data"."sba_loans" USING GIN ("borrname" public.gin_trgm_ops);
-- Create index "idx_sba_loans_name_upper" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_name_upper" ON "fed_data"."sba_loans" ((upper(TRIM(BOTH FROM borrname))));
-- Create index "idx_sba_loans_program" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_program" ON "fed_data"."sba_loans" ("program");
-- Create index "idx_sba_loans_state" to table: "sba_loans"
CREATE INDEX IF NOT EXISTS "idx_sba_loans_state" ON "fed_data"."sba_loans" ("borrstate");
-- Create "qcew_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."qcew_data" (
  "area_fips" character varying(5) NOT NULL,
  "own_code" character(1) NOT NULL,
  "industry_code" character varying(6) NOT NULL,
  "year" smallint NOT NULL,
  "qtr" smallint NOT NULL,
  "month1_emplvl" integer NULL,
  "month2_emplvl" integer NULL,
  "month3_emplvl" integer NULL,
  "total_qtrly_wages" bigint NULL,
  "avg_wkly_wage" integer NULL,
  "qtrly_estabs" integer NULL,
  PRIMARY KEY ("area_fips", "own_code", "industry_code", "year", "qtr")
);
-- Create index "idx_qcew_area" to table: "qcew_data"
CREATE INDEX IF NOT EXISTS "idx_qcew_area" ON "fed_data"."qcew_data" ("area_fips");
-- Create index "idx_qcew_area_industry" to table: "qcew_data"
CREATE INDEX IF NOT EXISTS "idx_qcew_area_industry" ON "fed_data"."qcew_data" ("area_fips", "industry_code");
-- Create index "idx_qcew_industry" to table: "qcew_data"
CREATE INDEX IF NOT EXISTS "idx_qcew_industry" ON "fed_data"."qcew_data" ("industry_code");
-- Create "ppp_loans" table
CREATE TABLE IF NOT EXISTS "fed_data"."ppp_loans" (
  "loannumber" bigint NOT NULL,
  "borrowername" text NOT NULL,
  "borroweraddress" text NULL,
  "borrowercity" text NULL,
  "borrowerstate" character(2) NULL,
  "borrowerzip" text NULL,
  "currentapprovalamount" numeric NULL,
  "forgivenessamount" numeric NULL,
  "jobsreported" integer NULL,
  "dateapproved" date NULL,
  "loanstatus" text NULL,
  "businesstype" text NULL,
  "naicscode" character varying(6) NULL,
  "businessagedescription" text NULL,
  PRIMARY KEY ("loannumber")
);
-- Create index "idx_ppp_name_trgm" to table: "ppp_loans"
CREATE INDEX IF NOT EXISTS "idx_ppp_name_trgm" ON "fed_data"."ppp_loans" USING GIN ("borrowername" public.gin_trgm_ops);
-- Create index "idx_ppp_name_upper" to table: "ppp_loans"
CREATE INDEX IF NOT EXISTS "idx_ppp_name_upper" ON "fed_data"."ppp_loans" ((upper(TRIM(BOTH FROM borrowername))));
-- Create index "idx_ppp_state" to table: "ppp_loans"
CREATE INDEX IF NOT EXISTS "idx_ppp_state" ON "fed_data"."ppp_loans" ("borrowerstate");
-- Create "adv_advisor_answers" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_advisor_answers" (
  "crd_number" integer NOT NULL,
  "question_key" character varying(80) NOT NULL,
  "value" jsonb NULL,
  "confidence" numeric(3,2) NULL,
  "tier" smallint NOT NULL,
  "reasoning" text NULL,
  "source_doc" character varying(20) NULL,
  "source_section" character varying(50) NULL,
  "model" character varying(50) NULL,
  "input_tokens" integer NOT NULL DEFAULT 0,
  "output_tokens" integer NOT NULL DEFAULT 0,
  "run_id" bigint NULL,
  "extracted_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "question_key"),
  CONSTRAINT "adv_advisor_answers_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "fed_data"."adv_extraction_runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_adv_advisor_answers_confidence" to table: "adv_advisor_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_advisor_answers_confidence" ON "fed_data"."adv_advisor_answers" ("confidence" DESC);
-- Create index "idx_adv_advisor_answers_run" to table: "adv_advisor_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_advisor_answers_run" ON "fed_data"."adv_advisor_answers" ("run_id");
-- Create index "idx_adv_advisor_answers_value" to table: "adv_advisor_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_advisor_answers_value" ON "fed_data"."adv_advisor_answers" USING GIN ("value");
-- Create "adv_fund_answers" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_fund_answers" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(20) NOT NULL,
  "question_key" character varying(80) NOT NULL,
  "value" jsonb NULL,
  "confidence" numeric(3,2) NULL,
  "tier" smallint NOT NULL,
  "reasoning" text NULL,
  "source_doc" character varying(20) NULL,
  "source_section" character varying(50) NULL,
  "model" character varying(50) NULL,
  "input_tokens" integer NOT NULL DEFAULT 0,
  "output_tokens" integer NOT NULL DEFAULT 0,
  "run_id" bigint NULL,
  "extracted_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "fund_id", "question_key"),
  CONSTRAINT "adv_fund_answers_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "fed_data"."adv_extraction_runs" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_adv_fund_answers_confidence" to table: "adv_fund_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_fund_answers_confidence" ON "fed_data"."adv_fund_answers" ("confidence" DESC);
-- Create index "idx_adv_fund_answers_run" to table: "adv_fund_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_fund_answers_run" ON "fed_data"."adv_fund_answers" ("run_id");
-- Create index "idx_adv_fund_answers_value" to table: "adv_fund_answers"
CREATE INDEX IF NOT EXISTS "idx_adv_fund_answers_value" ON "fed_data"."adv_fund_answers" USING GIN ("value");
-- Create "fdic_institutions" table
CREATE TABLE IF NOT EXISTS "fed_data"."fdic_institutions" (
  "cert" integer NOT NULL,
  "name" text NOT NULL,
  "active" integer NULL,
  "inactive" integer NULL,
  "address" text NULL,
  "address2" text NULL,
  "city" text NULL,
  "stalp" text NULL,
  "stname" text NULL,
  "zip" text NULL,
  "county" text NULL,
  "stnum" text NULL,
  "stcnty" text NULL,
  "latitude" numeric(10,7) NULL,
  "longitude" numeric(10,7) NULL,
  "cbsa" text NULL,
  "cbsa_no" text NULL,
  "cbsa_div" text NULL,
  "cbsa_div_no" text NULL,
  "cbsa_div_flg" text NULL,
  "cbsa_metro" text NULL,
  "cbsa_metro_flg" text NULL,
  "cbsa_metro_name" text NULL,
  "cbsa_micro_flg" text NULL,
  "csa" text NULL,
  "csa_no" text NULL,
  "csa_flg" text NULL,
  "bkclass" text NULL,
  "clcode" integer NULL,
  "specgrp" integer NULL,
  "instcat" integer NULL,
  "charter_class" text NULL,
  "cb" text NULL,
  "regagnt" text NULL,
  "regagent2" text NULL,
  "chrtagnt" text NULL,
  "charter" text NULL,
  "stchrtr" text NULL,
  "fedchrtr" text NULL,
  "fed" text NULL,
  "fed_rssd" text NULL,
  "fdicdbs" text NULL,
  "fdicregn" text NULL,
  "fdicsupv" text NULL,
  "suprv_fd" text NULL,
  "occdist" text NULL,
  "docket" text NULL,
  "cfpbflag" text NULL,
  "cfpbeffdte" text NULL,
  "cfpbenddte" text NULL,
  "insagnt1" text NULL,
  "insagnt2" text NULL,
  "insbif" text NULL,
  "inscoml" text NULL,
  "insdate" text NULL,
  "insdif" text NULL,
  "insfdic" integer NULL,
  "inssaif" text NULL,
  "inssave" text NULL,
  "asset" bigint NULL,
  "dep" bigint NULL,
  "depdom" bigint NULL,
  "eq" text NULL,
  "netinc" bigint NULL,
  "roa" numeric(10,4) NULL,
  "roe" numeric(10,4) NULL,
  "offices" integer NULL,
  "offdom" integer NULL,
  "offfor" integer NULL,
  "offoa" integer NULL,
  "webaddr" text NULL,
  "trust" text NULL,
  "estymd" text NULL,
  "endefymd" text NULL,
  "effdate" text NULL,
  "procdate" text NULL,
  "dateupdt" text NULL,
  "repdte" text NULL,
  "risdate" text NULL,
  "rundate" text NULL,
  "changec1" text NULL,
  "newcert" text NULL,
  "ultcert" text NULL,
  "priorname1" text NULL,
  "hctmult" text NULL,
  "namehcr" text NULL,
  "parcert" text NULL,
  "rssdhcr" text NULL,
  "cityhcr" text NULL,
  "stalphcr" text NULL,
  "conserve" text NULL,
  "mdi_status_code" text NULL,
  "mdi_status_desc" text NULL,
  "mutual" text NULL,
  "subchaps" text NULL,
  "oakar" text NULL,
  "sasser" text NULL,
  "law_sasser_flg" text NULL,
  "iba" text NULL,
  "qbprcoml" text NULL,
  "denovo" text NULL,
  "form31" text NULL,
  "te01n528" text NULL,
  "te02n528" text NULL,
  "te03n528" text NULL,
  "te04n528" text NULL,
  "te05n528" text NULL,
  "te06n528" text NULL,
  "te07n528" text NULL,
  "te08n528" text NULL,
  "te09n528" text NULL,
  "te10n528" text NULL,
  "te01n529" text NULL,
  "te02n529" text NULL,
  "te03n529" text NULL,
  "te04n529" text NULL,
  "te05n529" text NULL,
  "te06n529" text NULL,
  "uninum" text NULL,
  "oi" text NULL,
  "synced_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("cert")
);
-- Create index "idx_fdic_inst_active" to table: "fdic_institutions"
CREATE INDEX IF NOT EXISTS "idx_fdic_inst_active" ON "fed_data"."fdic_institutions" ("active");
-- Create index "idx_fdic_inst_asset" to table: "fdic_institutions"
CREATE INDEX IF NOT EXISTS "idx_fdic_inst_asset" ON "fed_data"."fdic_institutions" ("asset");
-- Create index "idx_fdic_inst_cbsa" to table: "fdic_institutions"
CREATE INDEX IF NOT EXISTS "idx_fdic_inst_cbsa" ON "fed_data"."fdic_institutions" ("cbsa_no");
-- Create index "idx_fdic_inst_name" to table: "fdic_institutions"
CREATE INDEX IF NOT EXISTS "idx_fdic_inst_name" ON "fed_data"."fdic_institutions" ("name");
-- Create index "idx_fdic_inst_state" to table: "fdic_institutions"
CREATE INDEX IF NOT EXISTS "idx_fdic_inst_state" ON "fed_data"."fdic_institutions" ("stalp");
-- Create "fdic_branches" table
CREATE TABLE IF NOT EXISTS "fed_data"."fdic_branches" (
  "uni_num" integer NOT NULL,
  "cert" integer NOT NULL,
  "name" text NULL,
  "off_name" text NULL,
  "off_num" text NULL,
  "fi_uninum" text NULL,
  "address" text NULL,
  "address2" text NULL,
  "city" text NULL,
  "stalp" text NULL,
  "stname" text NULL,
  "zip" text NULL,
  "county" text NULL,
  "stcnty" text NULL,
  "latitude" numeric(10,7) NULL,
  "longitude" numeric(10,7) NULL,
  "main_off" integer NULL,
  "bk_class" text NULL,
  "serv_type" integer NULL,
  "serv_type_desc" text NULL,
  "cbsa" text NULL,
  "cbsa_no" text NULL,
  "cbsa_div" text NULL,
  "cbsa_div_no" text NULL,
  "cbsa_div_flg" text NULL,
  "cbsa_metro" text NULL,
  "cbsa_metro_flg" text NULL,
  "cbsa_metro_name" text NULL,
  "cbsa_micro_flg" text NULL,
  "csa" text NULL,
  "csa_no" text NULL,
  "csa_flg" text NULL,
  "mdi_status_code" text NULL,
  "mdi_status_desc" text NULL,
  "run_date" text NULL,
  "estymd" text NULL,
  "acqdate" text NULL,
  "synced_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("uni_num"),
  CONSTRAINT "fdic_branches_cert_fkey" FOREIGN KEY ("cert") REFERENCES "fed_data"."fdic_institutions" ("cert") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create index "idx_fdic_branches_cbsa" to table: "fdic_branches"
CREATE INDEX IF NOT EXISTS "idx_fdic_branches_cbsa" ON "fed_data"."fdic_branches" ("cbsa_no");
-- Create index "idx_fdic_branches_cert" to table: "fdic_branches"
CREATE INDEX IF NOT EXISTS "idx_fdic_branches_cert" ON "fed_data"."fdic_branches" ("cert");
-- Create index "idx_fdic_branches_coords" to table: "fdic_branches"
CREATE INDEX IF NOT EXISTS "idx_fdic_branches_coords" ON "fed_data"."fdic_branches" ("latitude", "longitude") WHERE (latitude IS NOT NULL);
-- Create index "idx_fdic_branches_main" to table: "fdic_branches"
CREATE INDEX IF NOT EXISTS "idx_fdic_branches_main" ON "fed_data"."fdic_branches" ("cert") WHERE (main_off = 1);
-- Create index "idx_fdic_branches_state" to table: "fdic_branches"
CREATE INDEX IF NOT EXISTS "idx_fdic_branches_state" ON "fed_data"."fdic_branches" ("stalp");
-- Create "pe_firms" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_firms" (
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
CREATE INDEX IF NOT EXISTS "idx_pe_firms_firm_type" ON "fed_data"."pe_firms" ("firm_type");
-- Create index "idx_pe_firms_trgm" to table: "pe_firms"
CREATE INDEX IF NOT EXISTS "idx_pe_firms_trgm" ON "fed_data"."pe_firms" USING GIN ("firm_name" public.gin_trgm_ops);
-- Create "pe_extraction_runs" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_extraction_runs" (
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
CREATE INDEX IF NOT EXISTS "idx_pe_extraction_runs_firm" ON "fed_data"."pe_extraction_runs" ("pe_firm_id", "status");
-- Create "pe_answers" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_answers" (
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
CREATE INDEX IF NOT EXISTS "idx_pe_answers_run" ON "fed_data"."pe_answers" ("run_id");
-- Create index "idx_pe_answers_value" to table: "pe_answers"
CREATE INDEX IF NOT EXISTS "idx_pe_answers_value" ON "fed_data"."pe_answers" USING GIN ("value");
-- Create "pe_crawl_cache" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_crawl_cache" (
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
CREATE INDEX IF NOT EXISTS "idx_pe_crawl_cache_type" ON "fed_data"."pe_crawl_cache" ("pe_firm_id", "page_type");
-- Create "pe_firm_overrides" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_firm_overrides" (
  "pe_firm_id" bigint NOT NULL,
  "website_url_override" character varying(500) NOT NULL,
  "notes" text NULL,
  "created_by" character varying(100) NOT NULL DEFAULT 'manual',
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("pe_firm_id"),
  CONSTRAINT "pe_firm_overrides_pe_firm_id_fkey" FOREIGN KEY ("pe_firm_id") REFERENCES "fed_data"."pe_firms" ("pe_firm_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create "pe_firm_rias" table
CREATE TABLE IF NOT EXISTS "fed_data"."pe_firm_rias" (
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
CREATE INDEX IF NOT EXISTS "idx_pe_firm_rias_crd" ON "fed_data"."pe_firm_rias" ("crd_number");

-- Create "adv_brochure_sections" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_brochure_sections" (
    "crd_number" integer NOT NULL,
    "brochure_id" text NOT NULL,
    "section_key" text NOT NULL,
    "section_title" text,
    "text_content" text,
    "tables" jsonb,
    "metadata" jsonb,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("crd_number", "brochure_id", "section_key")
);
CREATE INDEX IF NOT EXISTS "idx_brochure_sections_crd"
    ON "fed_data"."adv_brochure_sections" ("crd_number");

-- Create "adv_crs_sections" table
CREATE TABLE IF NOT EXISTS "fed_data"."adv_crs_sections" (
    "crd_number" integer NOT NULL,
    "crs_id" text NOT NULL,
    "section_key" text NOT NULL,
    "section_title" text,
    "text_content" text,
    "tables" jsonb,
    "metadata" jsonb,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("crd_number", "crs_id", "section_key")
);
CREATE INDEX IF NOT EXISTS "idx_crs_sections_crd"
    ON "fed_data"."adv_crs_sections" ("crd_number");

-- +goose Down
-- Intentionally empty: never drop the entire schema.
