-- Public geo-related tables
-- Tables: cbsa_areas, msa_grid_cells, geocode_cache, discovery_candidates
-- Note: discovery_runs is included as it is referenced by discovery_candidates

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

-- Create "discovery_runs" table (referenced by discovery_candidates)
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
