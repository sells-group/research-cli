-- Add new schema named "public"
CREATE SCHEMA IF NOT EXISTS "public";
-- Set comment to schema: "public"
COMMENT ON SCHEMA "public" IS 'standard public schema';
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
-- Create "geocode_cache" table
CREATE TABLE "public"."geocode_cache" (
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
CREATE INDEX "idx_geocode_cache_at" ON "public"."geocode_cache" ("cached_at");
-- Set comment to table: "geocode_cache"
COMMENT ON TABLE "public"."geocode_cache" IS 'Caches PostGIS geocode() results keyed by SHA-256 of normalized address';
-- Create "checkpoints" table
CREATE TABLE "public"."checkpoints" (
  "company_id" text NOT NULL,
  "phase" text NOT NULL,
  "data" jsonb NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("company_id")
);
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
-- Create "companies" table
CREATE TABLE "public"."companies" (
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
CREATE UNIQUE INDEX "idx_companies_domain" ON "public"."companies" ("domain") WHERE ((domain)::text <> ''::text);
-- Create index "idx_companies_naics" to table: "companies"
CREATE INDEX "idx_companies_naics" ON "public"."companies" ("naics_code");
-- Create index "idx_companies_name_trgm" to table: "companies"
CREATE INDEX "idx_companies_name_trgm" ON "public"."companies" USING GIN ("name" public.gin_trgm_ops);
-- Create index "idx_companies_state" to table: "companies"
CREATE INDEX "idx_companies_state" ON "public"."companies" ("state");
-- Create "company_addresses" table
CREATE TABLE "public"."company_addresses" (
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
CREATE INDEX "idx_company_addresses_company" ON "public"."company_addresses" ("company_id");
-- Create index "idx_company_addresses_geom" to table: "company_addresses"
CREATE INDEX "idx_company_addresses_geom" ON "public"."company_addresses" USING GIST ("geom") WHERE (geom IS NOT NULL);
-- Create index "idx_company_addresses_state" to table: "company_addresses"
CREATE INDEX "idx_company_addresses_state" ON "public"."company_addresses" ("state");
-- Create "cbsa_areas" table
CREATE TABLE "public"."cbsa_areas" (
  "gid" serial NOT NULL,
  "cbsa_code" character varying(5) NOT NULL,
  "name" character varying(200) NOT NULL,
  "lsad" character varying(2) NULL,
  "geom" public.geometry(MultiPolygon,4326) NOT NULL,
  PRIMARY KEY ("gid"),
  CONSTRAINT "cbsa_areas_cbsa_code_key" UNIQUE ("cbsa_code")
);
-- Create index "idx_cbsa_code" to table: "cbsa_areas"
CREATE INDEX "idx_cbsa_code" ON "public"."cbsa_areas" ("cbsa_code");
-- Create index "idx_cbsa_geom" to table: "cbsa_areas"
CREATE INDEX "idx_cbsa_geom" ON "public"."cbsa_areas" USING GIST ("geom");
-- Create "address_msa" table
CREATE TABLE "public"."address_msa" (
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
CREATE INDEX "idx_addr_msa_address" ON "public"."address_msa" ("address_id");
-- Create index "idx_addr_msa_cbsa" to table: "address_msa"
CREATE INDEX "idx_addr_msa_cbsa" ON "public"."address_msa" ("cbsa_code");
-- Create index "idx_addr_msa_class" to table: "address_msa"
CREATE INDEX "idx_addr_msa_class" ON "public"."address_msa" ("classification");
-- Create "company_financials" table
CREATE TABLE "public"."company_financials" (
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
CREATE INDEX "idx_company_financials_company" ON "public"."company_financials" ("company_id", "metric");
-- Create index "idx_company_financials_period" to table: "company_financials"
CREATE INDEX "idx_company_financials_period" ON "public"."company_financials" ("period_date" DESC);
-- Create "company_identifiers" table
CREATE TABLE "public"."company_identifiers" (
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
CREATE INDEX "idx_company_identifiers_lookup" ON "public"."company_identifiers" ("system", "identifier");
-- Create "company_matches" table
CREATE TABLE "public"."company_matches" (
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
CREATE INDEX "idx_company_matches_source" ON "public"."company_matches" ("matched_source", "matched_key");
-- Create "company_sources" table
CREATE TABLE "public"."company_sources" (
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
CREATE INDEX "idx_company_sources_company" ON "public"."company_sources" ("company_id");
-- Create index "idx_company_sources_raw" to table: "company_sources"
CREATE INDEX "idx_company_sources_raw" ON "public"."company_sources" USING GIN ("raw_data");
-- Create "company_tags" table
CREATE TABLE "public"."company_tags" (
  "company_id" bigint NOT NULL,
  "tag_type" character varying(50) NOT NULL,
  "tag_value" character varying(200) NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("company_id", "tag_type", "tag_value"),
  CONSTRAINT "company_tags_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_company_tags_type" to table: "company_tags"
CREATE INDEX "idx_company_tags_type" ON "public"."company_tags" ("tag_type", "tag_value");
-- Create "contacts" table
CREATE TABLE "public"."contacts" (
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
CREATE INDEX "idx_contacts_company_role" ON "public"."contacts" ("company_id", "role_type");
-- Create index "idx_contacts_name_trgm" to table: "contacts"
CREATE INDEX "idx_contacts_name_trgm" ON "public"."contacts" USING GIN ("full_name" public.gin_trgm_ops);
-- Create "discovery_runs" table
CREATE TABLE "public"."discovery_runs" (
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
CREATE TABLE "public"."discovery_candidates" (
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
CREATE INDEX "idx_disc_cand_domain" ON "public"."discovery_candidates" ("domain") WHERE (domain IS NOT NULL);
-- Create index "idx_disc_cand_place" to table: "discovery_candidates"
CREATE INDEX "idx_disc_cand_place" ON "public"."discovery_candidates" ("google_place_id") WHERE (google_place_id IS NOT NULL);
-- Create index "idx_disc_cand_run" to table: "discovery_candidates"
CREATE INDEX "idx_disc_cand_run" ON "public"."discovery_candidates" ("run_id", "disqualified");
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
-- Create "licenses" table
CREATE TABLE "public"."licenses" (
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
CREATE INDEX "idx_licenses_company" ON "public"."licenses" ("company_id");
-- Create index "idx_licenses_type_state" to table: "licenses"
CREATE INDEX "idx_licenses_type_state" ON "public"."licenses" ("license_type", "state");
-- Create "msa_grid_cells" table
CREATE TABLE "public"."msa_grid_cells" (
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
CREATE INDEX "idx_grid_cbsa" ON "public"."msa_grid_cells" ("cbsa_code", "cell_km");
-- Create index "idx_grid_geom" to table: "msa_grid_cells"
CREATE INDEX "idx_grid_geom" ON "public"."msa_grid_cells" USING GIST ("geom");
-- Create index "idx_grid_unsearched" to table: "msa_grid_cells"
CREATE INDEX "idx_grid_unsearched" ON "public"."msa_grid_cells" ("cbsa_code") WHERE (searched_at IS NULL);
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
