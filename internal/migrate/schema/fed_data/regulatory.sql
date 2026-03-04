-- Regulatory tables (Phase 2)

-- Create "brokercheck" table
CREATE TABLE "fed_data"."brokercheck" (
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
CREATE INDEX "idx_brokercheck_crd" ON "fed_data"."brokercheck" ("crd_number");
-- Create index "idx_brokercheck_name" to table: "brokercheck"
CREATE INDEX "idx_brokercheck_name" ON "fed_data"."brokercheck" USING GIN ("firm_name" public.gin_trgm_ops);

-- Create "form_bd" table
CREATE TABLE "fed_data"."form_bd" (
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
CREATE TABLE "fed_data"."sec_enforcement_actions" (
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
CREATE INDEX "idx_enforcement_crd" ON "fed_data"."sec_enforcement_actions" ("crd_number");
-- Create index "idx_enforcement_date" to table: "sec_enforcement_actions"
CREATE INDEX "idx_enforcement_date" ON "fed_data"."sec_enforcement_actions" ("action_date" DESC);
-- Create index "idx_enforcement_respondent" to table: "sec_enforcement_actions"
CREATE INDEX "idx_enforcement_respondent" ON "fed_data"."sec_enforcement_actions" ("respondent_name");

-- Create "fdic_institutions" table
CREATE TABLE "fed_data"."fdic_institutions" (
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
CREATE INDEX "idx_fdic_inst_active" ON "fed_data"."fdic_institutions" ("active");
-- Create index "idx_fdic_inst_asset" to table: "fdic_institutions"
CREATE INDEX "idx_fdic_inst_asset" ON "fed_data"."fdic_institutions" ("asset");
-- Create index "idx_fdic_inst_cbsa" to table: "fdic_institutions"
CREATE INDEX "idx_fdic_inst_cbsa" ON "fed_data"."fdic_institutions" ("cbsa_no");
-- Create index "idx_fdic_inst_name" to table: "fdic_institutions"
CREATE INDEX "idx_fdic_inst_name" ON "fed_data"."fdic_institutions" ("name");
-- Create index "idx_fdic_inst_state" to table: "fdic_institutions"
CREATE INDEX "idx_fdic_inst_state" ON "fed_data"."fdic_institutions" ("stalp");

-- Create "fdic_branches" table
CREATE TABLE "fed_data"."fdic_branches" (
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
CREATE INDEX "idx_fdic_branches_cbsa" ON "fed_data"."fdic_branches" ("cbsa_no");
-- Create index "idx_fdic_branches_cert" to table: "fdic_branches"
CREATE INDEX "idx_fdic_branches_cert" ON "fed_data"."fdic_branches" ("cert");
-- Create index "idx_fdic_branches_coords" to table: "fdic_branches"
CREATE INDEX "idx_fdic_branches_coords" ON "fed_data"."fdic_branches" ("latitude", "longitude") WHERE (latitude IS NOT NULL);
-- Create index "idx_fdic_branches_main" to table: "fdic_branches"
CREATE INDEX "idx_fdic_branches_main" ON "fed_data"."fdic_branches" ("cert") WHERE (main_off = 1);
-- Create index "idx_fdic_branches_state" to table: "fdic_branches"
CREATE INDEX "idx_fdic_branches_state" ON "fed_data"."fdic_branches" ("stalp");
