-- schema/fed_data/reference.sql
-- Reference data tables (NAICS, FIPS, SIC)

-- Create "naics_codes" table
CREATE TABLE "fed_data"."naics_codes" (
  "code" character varying(6) NOT NULL,
  "title" character varying(300) NOT NULL,
  "sector" character(2) NOT NULL,
  "subsector" character(3) NULL,
  "industry_group" character(4) NULL,
  "description" text NULL,
  PRIMARY KEY ("code")
);
-- Create index "idx_naics_sector" to table: "naics_codes"
CREATE INDEX "idx_naics_sector" ON "fed_data"."naics_codes" ("sector");
-- Create "fips_codes" table
CREATE TABLE "fed_data"."fips_codes" (
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
CREATE INDEX "idx_fips_abbr" ON "fed_data"."fips_codes" ("state_abbr");
-- Create index "idx_fips_geoid" to table: "fips_codes"
CREATE INDEX "idx_fips_geoid" ON "fed_data"."fips_codes" ((((fips_state)::text || (fips_county)::text)));
-- Create index "idx_fips_state" to table: "fips_codes"
CREATE INDEX "idx_fips_state" ON "fed_data"."fips_codes" ("fips_state");
-- Create "sic_crosswalk" table
CREATE TABLE "fed_data"."sic_crosswalk" (
  "sic_code" character(4) NOT NULL,
  "sic_description" character varying(200) NULL,
  "naics_code" character varying(6) NOT NULL,
  "naics_description" character varying(300) NULL,
  PRIMARY KEY ("sic_code", "naics_code")
);
-- Create index "idx_sic_xwalk_naics" to table: "sic_crosswalk"
CREATE INDEX "idx_sic_xwalk_naics" ON "fed_data"."sic_crosswalk" ("naics_code");
-- Create index "idx_sic_xwalk_sic" to table: "sic_crosswalk"
CREATE INDEX "idx_sic_xwalk_sic" ON "fed_data"."sic_crosswalk" ("sic_code");
