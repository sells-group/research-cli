-- +goose Up
-- Federal data schema: sync log, market intel, SEC/EDGAR, compliance, regulatory, economic, reference, views.

-- schema/fed_data/schema.sql
-- Federal data schema and sync log

-- Add new schema named "fed_data"
CREATE SCHEMA IF NOT EXISTS "fed_data";
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

-- schema/fed_data/reference.sql
-- Reference data tables (NAICS, FIPS, SIC)

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

-- schema/fed_data/market_intel.sql
-- Market Intelligence tables (Phase 1)

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

-- Intelligence and scoring tables

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

-- SEC EDGAR tables (Phase 1B)

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

-- Compliance tables (Phase 2)

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

-- Regulatory tables (Phase 2)

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

-- Economic and on-demand tables (Phases 2-3)

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

-- Create "eci_data" table
CREATE TABLE IF NOT EXISTS "fed_data"."eci_data" (
  "series_id" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "period" character varying(3) NOT NULL,
  "value" numeric(10,1) NULL,
  PRIMARY KEY ("series_id", "year", "period")
);

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

-- Create "fred_series" table
CREATE TABLE IF NOT EXISTS "fed_data"."fred_series" (
  "series_id" character varying(30) NOT NULL,
  "obs_date" date NOT NULL,
  "value" numeric NULL,
  PRIMARY KEY ("series_id", "obs_date")
);
-- Create index "idx_fred_date" to table: "fred_series"
CREATE INDEX IF NOT EXISTS "idx_fred_date" ON "fed_data"."fred_series" ("obs_date" DESC);

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

-- schema/fed_data/views.sql
-- Federal data views (regular and materialized)

-- =============================================================================
-- Regular views
-- =============================================================================

CREATE OR REPLACE VIEW fed_data.v_custodian_advisors AS
SELECT cr.custodian_name,
    cr.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.num_accounts,
    f.state
FROM ((fed_data.adv_custodian_relationships cr
    JOIN fed_data.adv_firms f ON ((f.crd_number = cr.crd_number)))
    LEFT JOIN LATERAL ( SELECT fi2.aum_total,
            fi2.num_accounts
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = cr.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true));

CREATE OR REPLACE VIEW fed_data.v_custodian_market_share AS
SELECT cr.custodian_name,
    count(DISTINCT cr.crd_number) AS advisor_count,
    sum(fi.aum_total) AS total_aum,
    avg(fi.aum_total) AS avg_aum
FROM (fed_data.adv_custodian_relationships cr
    LEFT JOIN LATERAL ( SELECT fi2.aum_total
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = cr.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true))
GROUP BY cr.custodian_name;

CREATE OR REPLACE VIEW fed_data.v_service_provider_network AS
SELECT sp.provider_name,
    sp.provider_type,
    count(DISTINCT sp.crd_number) AS advisor_count,
    array_agg(DISTINCT f.firm_name ORDER BY f.firm_name) AS advisor_names
FROM (fed_data.adv_service_providers sp
    JOIN fed_data.adv_firms f ON ((f.crd_number = sp.crd_number)))
GROUP BY sp.provider_name, sp.provider_type;

-- =============================================================================
-- Materialized views
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_firm_combined AS
SELECT af.crd_number,
    af.firm_name,
    af.sec_number,
    af.city,
    af.state,
    af.website,
    lf.filing_date AS adv_filing_date,
    lf.aum,
    lf.num_accounts,
    lf.num_employees,
    lf.legal_name,
    lf.form_of_org,
    lf.total_employees AS detail_total_employees,
    lf.client_types,
    lf.comp_pct_aum,
    lf.comp_hourly,
    lf.comp_fixed,
    lf.comp_commissions,
    lf.comp_performance,
    lf.aum_discretionary,
    lf.aum_non_discretionary,
    lf.svc_financial_planning,
    lf.svc_portfolio_individuals,
    lf.svc_portfolio_pooled,
    lf.svc_portfolio_institutional,
    lf.svc_pension_consulting,
    lf.svc_adviser_selection,
    lf.wrap_fee_program,
    lf.financial_planning_clients,
    lf.biz_broker_dealer,
    lf.biz_insurance,
    lf.biz_real_estate,
    lf.biz_accountant,
    lf.aff_broker_dealer,
    lf.aff_bank,
    lf.aff_insurance,
    lf.sec_registered,
    lf.exempt_reporting,
    lf.state_registered,
    lf.custody_client_cash,
    lf.custody_client_securities,
    lf.has_any_drp,
    lf.drp_criminal_firm,
    lf.drp_regulatory_firm,
    bc.num_branch_offices,
    bc.num_registered_reps,
    bc.registration_status AS brokercheck_status,
    ex.cik,
    ex.match_type AS xref_match_type,
    ex.confidence AS xref_confidence,
    ee.sic,
    ee.sic_description,
    ee.tickers,
    ee.exchanges,
    be.investment_strategies,
    be.industry_specializations,
    be.min_account_size,
    be.fee_schedule,
    be.target_clients,
    ce.firm_type AS crs_firm_type,
    ce.key_services AS crs_key_services,
    ce.has_disciplinary_history AS crs_disciplinary_history
FROM ((((((fed_data.adv_firms af
    LEFT JOIN LATERAL ( SELECT adv_filings.crd_number,
            adv_filings.filing_date,
            adv_filings.aum,
            adv_filings.raum,
            adv_filings.num_accounts,
            adv_filings.num_employees,
            adv_filings.legal_name,
            adv_filings.form_of_org,
            adv_filings.num_other_offices,
            adv_filings.total_employees,
            adv_filings.num_adviser_reps,
            adv_filings.client_types,
            adv_filings.comp_pct_aum,
            adv_filings.comp_hourly,
            adv_filings.comp_subscription,
            adv_filings.comp_fixed,
            adv_filings.comp_commissions,
            adv_filings.comp_performance,
            adv_filings.comp_other,
            adv_filings.aum_discretionary,
            adv_filings.aum_non_discretionary,
            adv_filings.aum_total,
            adv_filings.svc_financial_planning,
            adv_filings.svc_portfolio_individuals,
            adv_filings.svc_portfolio_inv_cos,
            adv_filings.svc_portfolio_pooled,
            adv_filings.svc_portfolio_institutional,
            adv_filings.svc_pension_consulting,
            adv_filings.svc_adviser_selection,
            adv_filings.svc_periodicals,
            adv_filings.svc_security_ratings,
            adv_filings.svc_market_timing,
            adv_filings.svc_seminars,
            adv_filings.svc_other,
            adv_filings.wrap_fee_program,
            adv_filings.wrap_fee_raum,
            adv_filings.financial_planning_clients,
            adv_filings.biz_broker_dealer,
            adv_filings.biz_registered_rep,
            adv_filings.biz_cpo_cta,
            adv_filings.biz_futures_commission,
            adv_filings.biz_real_estate,
            adv_filings.biz_insurance,
            adv_filings.biz_bank,
            adv_filings.biz_trust_company,
            adv_filings.biz_municipal_advisor,
            adv_filings.biz_swap_dealer,
            adv_filings.biz_major_swap,
            adv_filings.biz_accountant,
            adv_filings.biz_lawyer,
            adv_filings.biz_other_financial,
            adv_filings.aff_broker_dealer,
            adv_filings.aff_other_adviser,
            adv_filings.aff_municipal_advisor,
            adv_filings.aff_swap_dealer,
            adv_filings.aff_major_swap,
            adv_filings.aff_cpo_cta,
            adv_filings.aff_futures_commission,
            adv_filings.aff_bank,
            adv_filings.aff_trust_company,
            adv_filings.aff_accountant,
            adv_filings.aff_lawyer,
            adv_filings.aff_insurance,
            adv_filings.aff_pension_consultant,
            adv_filings.aff_real_estate,
            adv_filings.aff_lp_sponsor,
            adv_filings.aff_pooled_vehicle,
            adv_filings.sec_registered,
            adv_filings.exempt_reporting,
            adv_filings.state_registered,
            adv_filings.discretionary_authority,
            adv_filings.txn_proprietary_interest,
            adv_filings.txn_sells_own_securities,
            adv_filings.txn_buys_from_clients,
            adv_filings.txn_recommends_own,
            adv_filings.txn_recommends_broker,
            adv_filings.txn_agency_cross,
            adv_filings.txn_principal,
            adv_filings.txn_referral_compensation,
            adv_filings.txn_other_research,
            adv_filings.txn_revenue_sharing,
            adv_filings.custody_client_cash,
            adv_filings.custody_client_securities,
            adv_filings.custody_related_person,
            adv_filings.custody_qualified_custodian,
            adv_filings.custody_surprise_exam,
            adv_filings.drp_criminal_firm,
            adv_filings.drp_criminal_affiliate,
            adv_filings.drp_regulatory_firm,
            adv_filings.drp_regulatory_affiliate,
            adv_filings.drp_civil_firm,
            adv_filings.drp_civil_affiliate,
            adv_filings.drp_complaint_firm,
            adv_filings.drp_complaint_affiliate,
            adv_filings.drp_termination_firm,
            adv_filings.drp_termination_affiliate,
            adv_filings.drp_judgment,
            adv_filings.drp_financial_firm,
            adv_filings.drp_financial_affiliate,
            adv_filings.has_any_drp,
            adv_filings.updated_at
        FROM fed_data.adv_filings
        WHERE (adv_filings.crd_number = af.crd_number)
        ORDER BY adv_filings.filing_date DESC
        LIMIT 1) lf ON (true))
    LEFT JOIN fed_data.brokercheck bc ON ((bc.crd_number = af.crd_number)))
    LEFT JOIN fed_data.entity_xref ex ON (((ex.crd_number = af.crd_number) AND (ex.confidence >= 0.7))))
    LEFT JOIN fed_data.edgar_entities ee ON (((ee.cik)::text = (ex.cik)::text)))
    LEFT JOIN LATERAL ( SELECT adv_brochure_enrichment.investment_strategies,
            adv_brochure_enrichment.industry_specializations,
            adv_brochure_enrichment.min_account_size,
            adv_brochure_enrichment.fee_schedule,
            adv_brochure_enrichment.target_clients
        FROM fed_data.adv_brochure_enrichment
        WHERE (adv_brochure_enrichment.crd_number = af.crd_number)
        ORDER BY adv_brochure_enrichment.enriched_at DESC
        LIMIT 1) be ON (true))
    LEFT JOIN LATERAL ( SELECT adv_crs_enrichment.firm_type,
            adv_crs_enrichment.key_services,
            adv_crs_enrichment.has_disciplinary_history
        FROM fed_data.adv_crs_enrichment
        WHERE (adv_crs_enrichment.crd_number = af.crd_number)
        ORDER BY adv_crs_enrichment.enriched_at DESC
        LIMIT 1) ce ON (true));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_firm_combined_crd ON fed_data.mv_firm_combined USING btree (crd_number);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_state ON fed_data.mv_firm_combined USING btree (state);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_aum ON fed_data.mv_firm_combined USING btree (aum DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mv_firm_combined_drp ON fed_data.mv_firm_combined USING btree (has_any_drp) WHERE (has_any_drp = true);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_market_size AS
SELECT c.naics,
    c.year,
    c.fips_state,
    sum(c.emp) AS total_emp,
    sum(c.est) AS total_est,
    sum(c.ap) AS total_payroll,
    q.avg_wkly_wage AS qcew_avg_weekly_wage,
    q.total_qtrly_wages AS qcew_qtrly_wages
FROM (fed_data.cbp_data c
    LEFT JOIN fed_data.qcew_data q ON ((((q.area_fips)::text = ((c.fips_state)::text || '000'::text)) AND ((q.industry_code)::text = (c.naics)::text) AND (q.year = c.year) AND (q.qtr = 1) AND (q.own_code = '5'::bpchar))))
WHERE (c.fips_county = '000'::bpchar)
GROUP BY c.naics, c.year, c.fips_state, q.avg_wkly_wage, q.total_qtrly_wages;

CREATE INDEX IF NOT EXISTS idx_mv_market_naics ON fed_data.mv_market_size USING btree (naics, year);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_13f_top_holders AS
SELECT f.cik,
    f.company_name,
    f.total_value,
    f.period_of_report,
    count(h.cusip) AS num_positions,
    sum(h.value) AS holdings_value
FROM (fed_data.f13_filers f
    JOIN fed_data.f13_holdings h ON ((((h.cik)::text = (f.cik)::text) AND (h.period = f.period_of_report))))
GROUP BY f.cik, f.company_name, f.total_value, f.period_of_report;

CREATE INDEX IF NOT EXISTS idx_mv_13f_value ON fed_data.mv_13f_top_holders USING btree (total_value DESC);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_adv_filing_history AS
SELECT crd_number,
    filing_date,
    aum_total,
    aum_discretionary,
    num_accounts,
    total_employees,
    num_adviser_reps,
    lag(aum_total) OVER w AS prior_aum,
    lag(num_accounts) OVER w AS prior_accounts,
    lag(total_employees) OVER w AS prior_employees,
    lag(filing_date) OVER w AS prior_filing_date
FROM fed_data.adv_filings
WHERE (filing_date IS NOT NULL)
WINDOW w AS (PARTITION BY crd_number ORDER BY filing_date);

CREATE INDEX IF NOT EXISTS idx_mv_filing_history_crd ON fed_data.mv_adv_filing_history USING btree (crd_number, filing_date);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_adv_intelligence AS
SELECT f.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.aum_discretionary,
    fi.num_accounts,
    fi.total_employees,
    f.city,
    f.state,
    f.website,
    fi.filing_date,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'fee_schedule_aum_tiers'::text))) AS fee_schedule,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'max_fee_rate_pct'::text))) AS max_fee_rate,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'min_fee_rate_pct'::text))) AS min_fee_rate,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'primary_custodian'::text))) AS primary_custodian,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'firm_specialization'::text))) AS firm_specialization,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'minimum_account_size'::text))) AS minimum_account_size,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'primary_investment_approach'::text))) AS investment_approach,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'ownership_structure_detail'::text))) AS ownership_structure,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'succession_plan_disclosed'::text))) AS succession_plan,
    ( SELECT a.value
        FROM fed_data.adv_advisor_answers a
        WHERE ((a.crd_number = f.crd_number) AND ((a.question_key)::text = 'key_personnel_names'::text))) AS key_personnel,
    cm.revenue_estimate AS computed_revenue,
    cm.blended_fee_rate_bps,
    cm.revenue_per_client AS computed_rev_per_client,
    cm.aum_growth_cagr_pct,
    cm.client_growth_rate_pct,
    cm.acquisition_readiness,
    cm.drp_severity,
    cm.business_complexity,
    cm.compensation_diversity,
    cm.fund_aum_pct_total,
    cm.aum_1yr_growth_pct,
    cm.aum_3yr_cagr_pct,
    cm.aum_5yr_cagr_pct,
    cm.concentration_risk_score,
    cm.key_person_dependency_score,
    cm.regulatory_risk_score,
    cm.hybrid_revenue_estimate,
    cm.estimated_operating_margin,
    cm.revenue_per_employee,
    ( SELECT count(*) AS count
        FROM fed_data.adv_private_funds pf
        WHERE (pf.crd_number = f.crd_number)) AS fund_count,
    ( SELECT sum(pf.gross_asset_value) AS sum
        FROM fed_data.adv_private_funds pf
        WHERE (pf.crd_number = f.crd_number)) AS total_fund_gav,
    ( SELECT count(*) AS count
        FROM fed_data.adv_advisor_answers a
        WHERE (a.crd_number = f.crd_number)) AS total_answers,
    ( SELECT avg(a.confidence) AS avg
        FROM fed_data.adv_advisor_answers a
        WHERE (a.crd_number = f.crd_number)) AS avg_confidence
FROM ((fed_data.adv_firms f
    LEFT JOIN LATERAL ( SELECT fi2.crd_number,
            fi2.filing_date,
            fi2.aum,
            fi2.raum,
            fi2.num_accounts,
            fi2.num_employees,
            fi2.legal_name,
            fi2.form_of_org,
            fi2.num_other_offices,
            fi2.total_employees,
            fi2.num_adviser_reps,
            fi2.client_types,
            fi2.comp_pct_aum,
            fi2.comp_hourly,
            fi2.comp_subscription,
            fi2.comp_fixed,
            fi2.comp_commissions,
            fi2.comp_performance,
            fi2.comp_other,
            fi2.aum_discretionary,
            fi2.aum_non_discretionary,
            fi2.aum_total,
            fi2.svc_financial_planning,
            fi2.svc_portfolio_individuals,
            fi2.svc_portfolio_inv_cos,
            fi2.svc_portfolio_pooled,
            fi2.svc_portfolio_institutional,
            fi2.svc_pension_consulting,
            fi2.svc_adviser_selection,
            fi2.svc_periodicals,
            fi2.svc_security_ratings,
            fi2.svc_market_timing,
            fi2.svc_seminars,
            fi2.svc_other,
            fi2.wrap_fee_program,
            fi2.wrap_fee_raum,
            fi2.financial_planning_clients,
            fi2.biz_broker_dealer,
            fi2.biz_registered_rep,
            fi2.biz_cpo_cta,
            fi2.biz_futures_commission,
            fi2.biz_real_estate,
            fi2.biz_insurance,
            fi2.biz_bank,
            fi2.biz_trust_company,
            fi2.biz_municipal_advisor,
            fi2.biz_swap_dealer,
            fi2.biz_major_swap,
            fi2.biz_accountant,
            fi2.biz_lawyer,
            fi2.biz_other_financial,
            fi2.aff_broker_dealer,
            fi2.aff_other_adviser,
            fi2.aff_municipal_advisor,
            fi2.aff_swap_dealer,
            fi2.aff_major_swap,
            fi2.aff_cpo_cta,
            fi2.aff_futures_commission,
            fi2.aff_bank,
            fi2.aff_trust_company,
            fi2.aff_accountant,
            fi2.aff_lawyer,
            fi2.aff_insurance,
            fi2.aff_pension_consultant,
            fi2.aff_real_estate,
            fi2.aff_lp_sponsor,
            fi2.aff_pooled_vehicle,
            fi2.sec_registered,
            fi2.exempt_reporting,
            fi2.state_registered,
            fi2.discretionary_authority,
            fi2.txn_proprietary_interest,
            fi2.txn_sells_own_securities,
            fi2.txn_buys_from_clients,
            fi2.txn_recommends_own,
            fi2.txn_recommends_broker,
            fi2.txn_agency_cross,
            fi2.txn_principal,
            fi2.txn_referral_compensation,
            fi2.txn_other_research,
            fi2.txn_revenue_sharing,
            fi2.custody_client_cash,
            fi2.custody_client_securities,
            fi2.custody_related_person,
            fi2.custody_qualified_custodian,
            fi2.custody_surprise_exam,
            fi2.drp_criminal_firm,
            fi2.drp_criminal_affiliate,
            fi2.drp_regulatory_firm,
            fi2.drp_regulatory_affiliate,
            fi2.drp_civil_firm,
            fi2.drp_civil_affiliate,
            fi2.drp_complaint_firm,
            fi2.drp_complaint_affiliate,
            fi2.drp_termination_firm,
            fi2.drp_termination_affiliate,
            fi2.drp_judgment,
            fi2.drp_financial_firm,
            fi2.drp_financial_affiliate,
            fi2.has_any_drp,
            fi2.updated_at,
            fi2.filing_type
        FROM fed_data.adv_filings fi2
        WHERE (fi2.crd_number = f.crd_number)
        ORDER BY fi2.filing_date DESC
        LIMIT 1) fi ON (true))
    LEFT JOIN fed_data.adv_computed_metrics cm ON ((cm.crd_number = f.crd_number)))
WHERE (EXISTS ( SELECT 1
    FROM fed_data.adv_advisor_answers a
    WHERE (a.crd_number = f.crd_number)));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_adv_intelligence_crd ON fed_data.mv_adv_intelligence USING btree (crd_number);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_state ON fed_data.mv_adv_intelligence USING btree (state);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_aum ON fed_data.mv_adv_intelligence USING btree (aum_total DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mv_adv_intelligence_readiness ON fed_data.mv_adv_intelligence USING btree (acquisition_readiness DESC NULLS LAST);

-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS fed_data.mv_pe_intelligence AS
SELECT pf.pe_firm_id,
    pf.firm_name,
    pf.firm_type,
    pf.website_url,
    pf.hq_city,
    pf.hq_state,
    pf.year_founded,
    pf.linkedin_url,
    pf.twitter_url,
    pf.facebook_url,
    pf.instagram_url,
    pf.youtube_url,
    pf.crunchbase_url,
    ria.ria_count,
    ria.total_ria_aum,
    ria.avg_ria_aum,
    ria.ria_states,
    array_length(ria.ria_states, 1) AS ria_state_count,
    geo.top_state,
    geo.top_state_aum_pct,
    ria.total_ria_accounts,
    ria.total_ria_employees,
    adv_agg.total_ria_revenue_estimate,
    adv_agg.avg_ria_revenue_per_client,
    adv_agg.avg_ria_operating_margin,
    adv_agg.avg_ria_aum_1yr_growth_pct,
    adv_agg.avg_ria_aum_3yr_cagr_pct,
    adv_agg.rias_with_drps,
    adv_agg.avg_regulatory_risk_score,
    adv_agg.avg_concentration_risk_score,
    adv_agg.avg_key_person_dependency,
    adv_agg.rias_discretionary_pct,
    svc_profile.most_common_services,
    comp_profile.most_common_compensation,
    top_rias.top_rias,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_firm_description'::text))) AS firm_description,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_firm_type'::text))) AS identified_firm_type,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_managing_partners'::text))) AS managing_partners,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_team_size'::text))) AS team_size,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_companies'::text))) AS portfolio_companies,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_count'::text))) AS portfolio_count,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_investment_strategy'::text))) AS investment_strategy,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_target_sectors'::text))) AS target_sectors,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_size_range'::text))) AS deal_size_range,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_total_aum'::text))) AS total_aum,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_fund_names'::text))) AS fund_names,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_exits_notable'::text))) AS notable_exits,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_acquisition_pattern'::text))) AS acquisition_pattern,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_strategic_assessment'::text))) AS strategic_assessment,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_contact_email'::text))) AS contact_email,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_contact_phone'::text))) AS contact_phone,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_recent_acquisitions'::text))) AS recent_acquisitions,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_velocity'::text))) AS deal_velocity,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_integration_approach'::text))) AS integration_approach,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_dry_powder'::text))) AS dry_powder,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_valuation_approach'::text))) AS valuation_approach,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_gap_analysis'::text))) AS portfolio_gap_analysis,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_competitive_position'::text))) AS competitive_position,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_investment_themes'::text))) AS investment_themes,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_market_views'::text))) AS market_views,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_target_profile_signals'::text))) AS target_profile_signals,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_deal_announcements'::text))) AS deal_announcements,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_fundraise_signals'::text))) AS fundraise_signals,
    ( SELECT pa.value
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_portfolio_updates'::text))) AS portfolio_updates,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_hiring_expansion'::text))) AS hiring_expansion,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_thought_leadership'::text))) AS thought_leadership,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_content_recency'::text))) AS content_recency,
    ( SELECT (pa.value #>> '{}'::text[])
        FROM fed_data.pe_answers pa
        WHERE ((pa.pe_firm_id = pf.pe_firm_id) AND ((pa.question_key)::text = 'pe_competitive_intel'::text))) AS competitive_intel,
    ans_stats.answer_count,
    ans_stats.avg_confidence,
    lr.latest_run_at,
    lr.latest_run_status,
    lr.latest_cost_usd
FROM ((((((((fed_data.pe_firms pf
    LEFT JOIN LATERAL ( SELECT count(*) AS ria_count,
            COALESCE(sum(fi.aum_total), (0)::numeric) AS total_ria_aum,
            COALESCE(avg(fi.aum_total), (0)::numeric) AS avg_ria_aum,
            COALESCE(sum(fi.num_accounts), (0)::bigint) AS total_ria_accounts,
            COALESCE(sum(fi.total_employees), (0)::bigint) AS total_ria_employees,
            array_agg(DISTINCT f.state) FILTER (WHERE (f.state IS NOT NULL)) AS ria_states
        FROM ((fed_data.pe_firm_rias pr
            JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.aum_total,
                    fi2.num_accounts,
                    fi2.total_employees
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE (pr.pe_firm_id = pf.pe_firm_id)) ria ON (true))
    LEFT JOIN LATERAL ( SELECT COALESCE(sum(acm.revenue_estimate), (0)::numeric) AS total_ria_revenue_estimate,
            avg(acm.revenue_per_client) AS avg_ria_revenue_per_client,
            avg(acm.estimated_operating_margin) AS avg_ria_operating_margin,
            avg(acm.aum_1yr_growth_pct) AS avg_ria_aum_1yr_growth_pct,
            avg(acm.aum_3yr_cagr_pct) AS avg_ria_aum_3yr_cagr_pct,
            count(*) FILTER (WHERE (fi.has_any_drp = true)) AS rias_with_drps,
            avg(acm.regulatory_risk_score) AS avg_regulatory_risk_score,
            avg(acm.concentration_risk_score) AS avg_concentration_risk_score,
            avg(acm.key_person_dependency_score) AS avg_key_person_dependency,
                CASE
                    WHEN (count(*) > 0) THEN round(((100.0 * (count(*) FILTER (WHERE (fi.discretionary_authority = true)))::numeric) / (count(*))::numeric), 1)
                    ELSE NULL::numeric
                END AS rias_discretionary_pct
        FROM ((fed_data.pe_firm_rias pr
            LEFT JOIN fed_data.adv_computed_metrics acm ON ((acm.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.has_any_drp,
                    fi2.discretionary_authority
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE (pr.pe_firm_id = pf.pe_firm_id)) adv_agg ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(common.service ORDER BY common.service) AS most_common_services
        FROM ( SELECT per_ria.service,
                count(*) AS cnt
            FROM ( SELECT pr.crd_number,
                    unnest(ARRAY[
                        CASE
                            WHEN fi.svc_financial_planning THEN 'financial_planning'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_individuals THEN 'portfolio_individuals'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_inv_cos THEN 'portfolio_inv_cos'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_pooled THEN 'portfolio_pooled'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_portfolio_institutional THEN 'portfolio_institutional'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_pension_consulting THEN 'pension_consulting'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_adviser_selection THEN 'adviser_selection'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_periodicals THEN 'periodicals'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_security_ratings THEN 'security_ratings'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_market_timing THEN 'market_timing'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_seminars THEN 'seminars'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.svc_other THEN 'other_services'::text
                            ELSE NULL::text
                        END]) AS service
                FROM (fed_data.pe_firm_rias pr
                    LEFT JOIN LATERAL ( SELECT fi2.svc_financial_planning,
                            fi2.svc_portfolio_individuals,
                            fi2.svc_portfolio_inv_cos,
                            fi2.svc_portfolio_pooled,
                            fi2.svc_portfolio_institutional,
                            fi2.svc_pension_consulting,
                            fi2.svc_adviser_selection,
                            fi2.svc_periodicals,
                            fi2.svc_security_ratings,
                            fi2.svc_market_timing,
                            fi2.svc_seminars,
                            fi2.svc_other
                        FROM fed_data.adv_filings fi2
                        WHERE (fi2.crd_number = pr.crd_number)
                        ORDER BY fi2.filing_date DESC
                        LIMIT 1) fi ON (true))
                WHERE (pr.pe_firm_id = pf.pe_firm_id)) per_ria
            WHERE (per_ria.service IS NOT NULL)
            GROUP BY per_ria.service
            HAVING ((count(*))::numeric > ((( SELECT count(*) AS count
                FROM fed_data.pe_firm_rias
                WHERE (pe_firm_rias.pe_firm_id = pf.pe_firm_id)))::numeric * 0.5))) common) svc_profile ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(common.comp_type ORDER BY common.comp_type) AS most_common_compensation
        FROM ( SELECT per_ria.comp_type,
                count(*) AS cnt
            FROM ( SELECT pr.crd_number,
                    unnest(ARRAY[
                        CASE
                            WHEN fi.comp_pct_aum THEN 'pct_aum'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_hourly THEN 'hourly'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_subscription THEN 'subscription'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_fixed THEN 'fixed'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_commissions THEN 'commissions'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_performance THEN 'performance'::text
                            ELSE NULL::text
                        END,
                        CASE
                            WHEN fi.comp_other THEN 'other_comp'::text
                            ELSE NULL::text
                        END]) AS comp_type
                FROM (fed_data.pe_firm_rias pr
                    LEFT JOIN LATERAL ( SELECT fi2.comp_pct_aum,
                            fi2.comp_hourly,
                            fi2.comp_subscription,
                            fi2.comp_fixed,
                            fi2.comp_commissions,
                            fi2.comp_performance,
                            fi2.comp_other
                        FROM fed_data.adv_filings fi2
                        WHERE (fi2.crd_number = pr.crd_number)
                        ORDER BY fi2.filing_date DESC
                        LIMIT 1) fi ON (true))
                WHERE (pr.pe_firm_id = pf.pe_firm_id)) per_ria
            WHERE (per_ria.comp_type IS NOT NULL)
            GROUP BY per_ria.comp_type
            HAVING ((count(*))::numeric > ((( SELECT count(*) AS count
                FROM fed_data.pe_firm_rias
                WHERE (pe_firm_rias.pe_firm_id = pf.pe_firm_id)))::numeric * 0.5))) common) comp_profile ON (true))
    LEFT JOIN LATERAL ( SELECT jsonb_agg(sub.ria_row) AS top_rias
        FROM ( SELECT jsonb_build_object('crd', pr.crd_number, 'name', f.firm_name, 'aum', fi.aum_total, 'state', f.state, 'employees', fi.total_employees) AS ria_row
            FROM ((fed_data.pe_firm_rias pr
                JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
                LEFT JOIN LATERAL ( SELECT fi2.aum_total,
                        fi2.total_employees
                    FROM fed_data.adv_filings fi2
                    WHERE (fi2.crd_number = pr.crd_number)
                    ORDER BY fi2.filing_date DESC
                    LIMIT 1) fi ON (true))
            WHERE (pr.pe_firm_id = pf.pe_firm_id)
            ORDER BY fi.aum_total DESC NULLS LAST
            LIMIT 5) sub) top_rias ON (true))
    LEFT JOIN LATERAL ( SELECT f.state AS top_state,
            round(((100.0 * sum(fi.aum_total)) / NULLIF(ria.total_ria_aum, (0)::numeric)), 1) AS top_state_aum_pct
        FROM ((fed_data.pe_firm_rias pr
            JOIN fed_data.adv_firms f ON ((f.crd_number = pr.crd_number)))
            LEFT JOIN LATERAL ( SELECT fi2.aum_total
                FROM fed_data.adv_filings fi2
                WHERE (fi2.crd_number = pr.crd_number)
                ORDER BY fi2.filing_date DESC
                LIMIT 1) fi ON (true))
        WHERE ((pr.pe_firm_id = pf.pe_firm_id) AND (f.state IS NOT NULL))
        GROUP BY f.state
        ORDER BY (sum(fi.aum_total)) DESC NULLS LAST
        LIMIT 1) geo ON (true))
    LEFT JOIN LATERAL ( SELECT count(*) AS answer_count,
            avg(pa.confidence) AS avg_confidence
        FROM fed_data.pe_answers pa
        WHERE (pa.pe_firm_id = pf.pe_firm_id)) ans_stats ON (true))
    LEFT JOIN LATERAL ( SELECT er.completed_at AS latest_run_at,
            er.status AS latest_run_status,
            er.cost_usd AS latest_cost_usd
        FROM fed_data.pe_extraction_runs er
        WHERE (er.pe_firm_id = pf.pe_firm_id)
        ORDER BY er.started_at DESC
        LIMIT 1) lr ON (true));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pe_intelligence_pk ON fed_data.mv_pe_intelligence USING btree (pe_firm_id);
CREATE INDEX IF NOT EXISTS idx_mv_pe_intelligence_ria_count ON fed_data.mv_pe_intelligence USING btree (ria_count DESC);
CREATE INDEX IF NOT EXISTS idx_mv_pe_intelligence_aum ON fed_data.mv_pe_intelligence USING btree (total_ria_aum DESC);

-- +goose Down
-- Initial schema migration: no rollback.
