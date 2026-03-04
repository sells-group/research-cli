-- Economic and on-demand tables (Phases 2-3)

-- Create "nes_data" table
CREATE TABLE "fed_data"."nes_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "firmpdemp" integer NULL,
  "rcppdemp" bigint NULL,
  "payann_pct" numeric(8,2) NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);

-- Create "asm_data" table
CREATE TABLE "fed_data"."asm_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "valadd" bigint NULL,
  "totval_ship" bigint NULL,
  "prodwrkrs" integer NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);

-- Create "eci_data" table
CREATE TABLE "fed_data"."eci_data" (
  "series_id" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "period" character varying(3) NOT NULL,
  "value" numeric(10,1) NULL,
  PRIMARY KEY ("series_id", "year", "period")
);

-- Create "abs_data" table
CREATE TABLE "fed_data"."abs_data" (
  "year" smallint NOT NULL,
  "naics" character varying(6) NOT NULL,
  "geo_id" character varying(15) NOT NULL,
  "firmpdemp" integer NULL,
  "rcppdemp" bigint NULL,
  "payann" bigint NULL,
  PRIMARY KEY ("year", "naics", "geo_id")
);

-- Create "laus_data" table
CREATE TABLE "fed_data"."laus_data" (
  "series_id" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "period" character varying(3) NOT NULL,
  "value" numeric(12,1) NULL,
  PRIMARY KEY ("series_id", "year", "period")
);

-- Create "m3_data" table
CREATE TABLE "fed_data"."m3_data" (
  "category" character varying(50) NOT NULL,
  "data_type" character varying(20) NOT NULL,
  "year" smallint NOT NULL,
  "month" smallint NOT NULL,
  "value" bigint NULL,
  PRIMARY KEY ("category", "data_type", "year", "month")
);

-- Create "fred_series" table
CREATE TABLE "fed_data"."fred_series" (
  "series_id" character varying(30) NOT NULL,
  "obs_date" date NOT NULL,
  "value" numeric NULL,
  PRIMARY KEY ("series_id", "obs_date")
);
-- Create index "idx_fred_date" to table: "fred_series"
CREATE INDEX "idx_fred_date" ON "fed_data"."fred_series" ("obs_date" DESC);

-- Create "xbrl_facts" table
CREATE TABLE "fed_data"."xbrl_facts" (
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
CREATE INDEX "idx_xbrl_cik_fact" ON "fed_data"."xbrl_facts" ("cik", "fact_name");
-- Create index "idx_xbrl_facts_cik" to table: "xbrl_facts"
CREATE INDEX "idx_xbrl_facts_cik" ON "fed_data"."xbrl_facts" ("cik");

-- Create "usaspending_awards" table
CREATE TABLE "fed_data"."usaspending_awards" (
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
CREATE INDEX "idx_usaspending_action_date" ON "fed_data"."usaspending_awards" ("award_latest_action_date" DESC);
-- Create index "idx_usaspending_agency" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_agency" ON "fed_data"."usaspending_awards" ("awarding_agency_code");
-- Create index "idx_usaspending_cfda" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_cfda" ON "fed_data"."usaspending_awards" ("cfda_number");
-- Create index "idx_usaspending_modified" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_modified" ON "fed_data"."usaspending_awards" ("last_modified_date" DESC);
-- Create index "idx_usaspending_naics" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_naics" ON "fed_data"."usaspending_awards" ("naics_code");
-- Create index "idx_usaspending_name_trgm" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_name_trgm" ON "fed_data"."usaspending_awards" USING GIN ("recipient_name" public.gin_trgm_ops);
-- Create index "idx_usaspending_recipient_duns" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_recipient_duns" ON "fed_data"."usaspending_awards" ("recipient_duns");
-- Create index "idx_usaspending_recipient_uei" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_recipient_uei" ON "fed_data"."usaspending_awards" ("recipient_uei");
-- Create index "idx_usaspending_state" to table: "usaspending_awards"
CREATE INDEX "idx_usaspending_state" ON "fed_data"."usaspending_awards" ("recipient_state");

-- Create "ncua_call_reports" table
CREATE TABLE "fed_data"."ncua_call_reports" (
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
CREATE INDEX "idx_ncua_call_reports_cu_name_trgm" ON "fed_data"."ncua_call_reports" USING GIN ("cu_name" public.gin_trgm_ops);
-- Create index "idx_ncua_call_reports_cu_number" to table: "ncua_call_reports"
CREATE INDEX "idx_ncua_call_reports_cu_number" ON "fed_data"."ncua_call_reports" ("cu_number");
-- Create index "idx_ncua_call_reports_cycle_date" to table: "ncua_call_reports"
CREATE INDEX "idx_ncua_call_reports_cycle_date" ON "fed_data"."ncua_call_reports" ("cycle_date" DESC);
-- Create index "idx_ncua_call_reports_state" to table: "ncua_call_reports"
CREATE INDEX "idx_ncua_call_reports_state" ON "fed_data"."ncua_call_reports" ("state");
-- Create index "idx_ncua_call_reports_total_assets" to table: "ncua_call_reports"
CREATE INDEX "idx_ncua_call_reports_total_assets" ON "fed_data"."ncua_call_reports" ("total_assets" DESC);
