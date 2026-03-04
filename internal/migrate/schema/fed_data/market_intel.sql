-- schema/fed_data/market_intel.sql
-- Market Intelligence tables (Phase 1)

-- Create "cbp_data" table
CREATE TABLE "fed_data"."cbp_data" (
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
CREATE INDEX "idx_cbp_fips" ON "fed_data"."cbp_data" ("fips_state", "fips_county");
-- Create index "idx_cbp_naics" to table: "cbp_data"
CREATE INDEX "idx_cbp_naics" ON "fed_data"."cbp_data" ("naics");
-- Create "susb_data" table
CREATE TABLE "fed_data"."susb_data" (
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
CREATE INDEX "idx_susb_naics" ON "fed_data"."susb_data" ("naics");
-- Create "qcew_data" table
CREATE TABLE "fed_data"."qcew_data" (
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
CREATE INDEX "idx_qcew_area" ON "fed_data"."qcew_data" ("area_fips");
-- Create index "idx_qcew_area_industry" to table: "qcew_data"
CREATE INDEX "idx_qcew_area_industry" ON "fed_data"."qcew_data" ("area_fips", "industry_code");
-- Create index "idx_qcew_industry" to table: "qcew_data"
CREATE INDEX "idx_qcew_industry" ON "fed_data"."qcew_data" ("industry_code");
-- Create "oews_data" table
CREATE TABLE "fed_data"."oews_data" (
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
CREATE INDEX "idx_oews_naics" ON "fed_data"."oews_data" ("naics");
-- Create index "idx_oews_occ" to table: "oews_data"
CREATE INDEX "idx_oews_occ" ON "fed_data"."oews_data" ("occ_code");
-- Create "fpds_contracts" table
CREATE TABLE "fed_data"."fpds_contracts" (
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
CREATE INDEX "idx_fpds_date" ON "fed_data"."fpds_contracts" ("date_signed");
-- Create index "idx_fpds_naics" to table: "fpds_contracts"
CREATE INDEX "idx_fpds_naics" ON "fed_data"."fpds_contracts" ("naics");
-- Create index "idx_fpds_vendor_name" to table: "fpds_contracts"
CREATE INDEX "idx_fpds_vendor_name" ON "fed_data"."fpds_contracts" USING GIN ("vendor_name" public.gin_trgm_ops);
-- Create index "idx_fpds_vendor_state" to table: "fpds_contracts"
CREATE INDEX "idx_fpds_vendor_state" ON "fed_data"."fpds_contracts" ("vendor_state");
-- Create index "idx_fpds_vendor_uei" to table: "fpds_contracts"
CREATE INDEX "idx_fpds_vendor_uei" ON "fed_data"."fpds_contracts" ("vendor_uei");
-- Create "economic_census" table
CREATE TABLE "fed_data"."economic_census" (
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
CREATE INDEX "idx_econcensus_naics" ON "fed_data"."economic_census" ("naics");
-- Create "ppp_loans" table
CREATE TABLE "fed_data"."ppp_loans" (
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
CREATE INDEX "idx_ppp_name_trgm" ON "fed_data"."ppp_loans" USING GIN ("borrowername" public.gin_trgm_ops);
-- Create index "idx_ppp_name_upper" to table: "ppp_loans"
CREATE INDEX "idx_ppp_name_upper" ON "fed_data"."ppp_loans" ((upper(TRIM(BOTH FROM borrowername))));
-- Create index "idx_ppp_state" to table: "ppp_loans"
CREATE INDEX "idx_ppp_state" ON "fed_data"."ppp_loans" ("borrowerstate");
-- Create "sba_loans" table
CREATE TABLE "fed_data"."sba_loans" (
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
CREATE INDEX "idx_sba_loans_fdic" ON "fed_data"."sba_loans" ("bankfdicnumber") WHERE (bankfdicnumber IS NOT NULL);
-- Create index "idx_sba_loans_fy" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_fy" ON "fed_data"."sba_loans" ("approvalfiscalyear");
-- Create index "idx_sba_loans_naics" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_naics" ON "fed_data"."sba_loans" ("naicscode");
-- Create index "idx_sba_loans_name_trgm" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_name_trgm" ON "fed_data"."sba_loans" USING GIN ("borrname" public.gin_trgm_ops);
-- Create index "idx_sba_loans_name_upper" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_name_upper" ON "fed_data"."sba_loans" ((upper(TRIM(BOTH FROM borrname))));
-- Create index "idx_sba_loans_program" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_program" ON "fed_data"."sba_loans" ("program");
-- Create index "idx_sba_loans_state" to table: "sba_loans"
CREATE INDEX "idx_sba_loans_state" ON "fed_data"."sba_loans" ("borrstate");
