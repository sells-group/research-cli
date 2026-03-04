-- Company and address tables
-- Tables: companies, company_identifiers, company_addresses, contacts, licenses,
--         company_sources, company_financials, company_tags, company_matches, address_msa

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
