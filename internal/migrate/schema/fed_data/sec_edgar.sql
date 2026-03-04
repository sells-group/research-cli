-- SEC EDGAR tables (Phase 1B)

-- Create "adv_firms" table
CREATE TABLE "fed_data"."adv_firms" (
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
CREATE INDEX "idx_adv_firms_name" ON "fed_data"."adv_firms" USING GIN ("firm_name" public.gin_trgm_ops);
-- Create index "idx_adv_firms_state" to table: "adv_firms"
CREATE INDEX "idx_adv_firms_state" ON "fed_data"."adv_firms" ("state");

-- Create "adv_filings" table
CREATE TABLE "fed_data"."adv_filings" (
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
CREATE INDEX "idx_adv_filings_aum" ON "fed_data"."adv_filings" ("aum" DESC NULLS LAST);
-- Create index "idx_adv_filings_date" to table: "adv_filings"
CREATE INDEX "idx_adv_filings_date" ON "fed_data"."adv_filings" ("filing_date" DESC);
-- Create index "idx_adv_filings_drp" to table: "adv_filings"
CREATE INDEX "idx_adv_filings_drp" ON "fed_data"."adv_filings" ("has_any_drp") WHERE (has_any_drp = true);
-- Set comment to column: "filing_type" on table: "adv_filings"
COMMENT ON COLUMN "fed_data"."adv_filings"."filing_type" IS 'Filing type: annual, amendment, initial, etc.';

-- Create "adv_answer_history" table
CREATE TABLE "fed_data"."adv_answer_history" (
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
CREATE INDEX "idx_answer_hist_crd" ON "fed_data"."adv_answer_history" ("crd_number", "question_key");

-- Create "adv_bd_affiliations" table
CREATE TABLE "fed_data"."adv_bd_affiliations" (
  "crd_number" integer NOT NULL,
  "bd_name" character varying(200) NOT NULL,
  "bd_crd" integer NULL,
  "relationship" character varying(50) NULL DEFAULT 'affiliated',
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "bd_name")
);
-- Create index "idx_bd_aff_name" to table: "adv_bd_affiliations"
CREATE INDEX "idx_bd_aff_name" ON "fed_data"."adv_bd_affiliations" ("bd_name");

-- Create "adv_brochure_enrichment" table
CREATE TABLE "fed_data"."adv_brochure_enrichment" (
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
CREATE INDEX "idx_brochure_enrich_industries" ON "fed_data"."adv_brochure_enrichment" USING GIN ("industry_specializations");
-- Create index "idx_brochure_enrich_strategies" to table: "adv_brochure_enrichment"
CREATE INDEX "idx_brochure_enrich_strategies" ON "fed_data"."adv_brochure_enrichment" USING GIN ("investment_strategies");

-- Create "adv_brochures" table
CREATE TABLE "fed_data"."adv_brochures" (
  "crd_number" integer NOT NULL,
  "brochure_id" character varying(50) NOT NULL,
  "filing_date" date NULL,
  "text_content" text NULL,
  "extracted_at" timestamptz NULL,
  PRIMARY KEY ("crd_number", "brochure_id")
);

-- Create "adv_computed_metrics" table
CREATE TABLE "fed_data"."adv_computed_metrics" (
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
CREATE TABLE "fed_data"."adv_crs" (
  "crd_number" integer NOT NULL,
  "crs_id" character varying(50) NOT NULL,
  "filing_date" date NULL,
  "text_content" text NULL,
  "extracted_at" timestamptz NULL,
  PRIMARY KEY ("crd_number", "crs_id")
);

-- Create "adv_crs_enrichment" table
CREATE TABLE "fed_data"."adv_crs_enrichment" (
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
CREATE INDEX "idx_crs_enrich_firm_type" ON "fed_data"."adv_crs_enrichment" ("firm_type");

-- Create "adv_custodian_relationships" table
CREATE TABLE "fed_data"."adv_custodian_relationships" (
  "crd_number" integer NOT NULL,
  "custodian_name" character varying(200) NOT NULL,
  "relationship" character varying(50) NULL DEFAULT 'custodian',
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "custodian_name")
);
-- Create index "idx_custodian_rel_name" to table: "adv_custodian_relationships"
CREATE INDEX "idx_custodian_rel_name" ON "fed_data"."adv_custodian_relationships" ("custodian_name");

-- Create "adv_disclosures" table
CREATE TABLE "fed_data"."adv_disclosures" (
  "crd_number" integer NOT NULL,
  "disclosure_type" character varying(100) NOT NULL,
  "event_date" date NULL,
  "description" text NULL,
  "id" bigserial NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_adv_disclosures_crd" to table: "adv_disclosures"
CREATE INDEX "idx_adv_disclosures_crd" ON "fed_data"."adv_disclosures" ("crd_number");

-- Create "adv_document_sections" table
CREATE TABLE "fed_data"."adv_document_sections" (
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
CREATE TABLE "fed_data"."adv_fund_filings" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(50) NOT NULL,
  "filing_date" date NOT NULL,
  "gross_asset_value" bigint NULL,
  "net_asset_value" bigint NULL,
  "fund_type" character varying(100) NULL,
  PRIMARY KEY ("crd_number", "fund_id", "filing_date")
);

-- Create "adv_fund_performance" table
CREATE TABLE "fed_data"."adv_fund_performance" (
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
CREATE INDEX "idx_fund_perf_crd" ON "fed_data"."adv_fund_performance" ("crd_number");

-- Create "adv_owners" table
CREATE TABLE "fed_data"."adv_owners" (
  "crd_number" integer NOT NULL,
  "owner_name" character varying(200) NOT NULL,
  "owner_type" character varying(50) NULL,
  "ownership_pct" numeric(5,2) NULL,
  "is_control" boolean NULL DEFAULT false,
  PRIMARY KEY ("crd_number", "owner_name")
);
-- Create index "idx_adv_owners_name" to table: "adv_owners"
CREATE INDEX "idx_adv_owners_name" ON "fed_data"."adv_owners" USING GIN ("owner_name" public.gin_trgm_ops);

-- Create "adv_private_funds" table
CREATE TABLE "fed_data"."adv_private_funds" (
  "crd_number" integer NOT NULL,
  "fund_id" character varying(50) NOT NULL,
  "fund_name" character varying(300) NULL,
  "fund_type" character varying(100) NULL,
  "gross_asset_value" bigint NULL,
  "net_asset_value" bigint NULL,
  PRIMARY KEY ("crd_number", "fund_id")
);

-- Create "adv_service_providers" table
CREATE TABLE "fed_data"."adv_service_providers" (
  "crd_number" integer NOT NULL,
  "provider_name" character varying(200) NOT NULL,
  "provider_type" character varying(50) NOT NULL,
  "updated_at" timestamptz NULL DEFAULT now(),
  PRIMARY KEY ("crd_number", "provider_name", "provider_type")
);
-- Create index "idx_svc_provider_type" to table: "adv_service_providers"
CREATE INDEX "idx_svc_provider_type" ON "fed_data"."adv_service_providers" ("provider_type");

-- Create "adv_extraction_runs" table
CREATE TABLE "fed_data"."adv_extraction_runs" (
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
CREATE INDEX "idx_adv_extraction_runs_crd" ON "fed_data"."adv_extraction_runs" ("crd_number");
-- Create index "idx_adv_extraction_runs_status" to table: "adv_extraction_runs"
CREATE INDEX "idx_adv_extraction_runs_status" ON "fed_data"."adv_extraction_runs" ("status");

-- Create "adv_advisor_answers" table
CREATE TABLE "fed_data"."adv_advisor_answers" (
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
CREATE INDEX "idx_adv_advisor_answers_confidence" ON "fed_data"."adv_advisor_answers" ("confidence" DESC);
-- Create index "idx_adv_advisor_answers_run" to table: "adv_advisor_answers"
CREATE INDEX "idx_adv_advisor_answers_run" ON "fed_data"."adv_advisor_answers" ("run_id");
-- Create index "idx_adv_advisor_answers_value" to table: "adv_advisor_answers"
CREATE INDEX "idx_adv_advisor_answers_value" ON "fed_data"."adv_advisor_answers" USING GIN ("value");

-- Create "adv_fund_answers" table
CREATE TABLE "fed_data"."adv_fund_answers" (
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
CREATE INDEX "idx_adv_fund_answers_confidence" ON "fed_data"."adv_fund_answers" ("confidence" DESC);
-- Create index "idx_adv_fund_answers_run" to table: "adv_fund_answers"
CREATE INDEX "idx_adv_fund_answers_run" ON "fed_data"."adv_fund_answers" ("run_id");
-- Create index "idx_adv_fund_answers_value" to table: "adv_fund_answers"
CREATE INDEX "idx_adv_fund_answers_value" ON "fed_data"."adv_fund_answers" USING GIN ("value");

-- Create "f13_filers" table
CREATE TABLE "fed_data"."f13_filers" (
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
CREATE INDEX "idx_f13_filers_name" ON "fed_data"."f13_filers" USING GIN ("company_name" public.gin_trgm_ops);

-- Create "f13_holdings" table
CREATE TABLE "fed_data"."f13_holdings" (
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
CREATE INDEX "idx_f13_holdings_cusip" ON "fed_data"."f13_holdings" ("cusip");

-- Create "form_d" table
CREATE TABLE "fed_data"."form_d" (
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
CREATE INDEX "idx_form_d_cik" ON "fed_data"."form_d" ("cik");
-- Create index "idx_form_d_name" to table: "form_d"
CREATE INDEX "idx_form_d_name" ON "fed_data"."form_d" USING GIN ("entity_name" public.gin_trgm_ops);

-- Create "edgar_entities" table
CREATE TABLE "fed_data"."edgar_entities" (
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
CREATE INDEX "idx_edgar_entities_name" ON "fed_data"."edgar_entities" USING GIN ("entity_name" public.gin_trgm_ops);
-- Create index "idx_edgar_entities_sic" to table: "edgar_entities"
CREATE INDEX "idx_edgar_entities_sic" ON "fed_data"."edgar_entities" ("sic");

-- Create "edgar_filings" table
CREATE TABLE "fed_data"."edgar_filings" (
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
CREATE INDEX "idx_edgar_filings_cik" ON "fed_data"."edgar_filings" ("cik");
-- Create index "idx_edgar_filings_date" to table: "edgar_filings"
CREATE INDEX "idx_edgar_filings_date" ON "fed_data"."edgar_filings" ("filing_date");
-- Create index "idx_edgar_filings_date_form" to table: "edgar_filings"
CREATE INDEX "idx_edgar_filings_date_form" ON "fed_data"."edgar_filings" ("filing_date" DESC, "form_type");
-- Create index "idx_edgar_filings_form" to table: "edgar_filings"
CREATE INDEX "idx_edgar_filings_form" ON "fed_data"."edgar_filings" ("form_type");

-- Create "entity_xref" table
CREATE TABLE "fed_data"."entity_xref" (
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
CREATE INDEX "idx_entity_xref_cik" ON "fed_data"."entity_xref" ("cik");
-- Create index "idx_entity_xref_crd" to table: "entity_xref"
CREATE INDEX "idx_entity_xref_crd" ON "fed_data"."entity_xref" ("crd_number");
-- Create index "idx_entity_xref_crd_cik" to table: "entity_xref"
CREATE UNIQUE INDEX "idx_entity_xref_crd_cik" ON "fed_data"."entity_xref" ("crd_number", "cik") WHERE ((crd_number IS NOT NULL) AND (cik IS NOT NULL));
-- Create index "idx_entity_xref_match" to table: "entity_xref"
CREATE INDEX "idx_entity_xref_match" ON "fed_data"."entity_xref" ("match_type", "confidence" DESC);

-- Create "entity_xref_multi" table
CREATE TABLE "fed_data"."entity_xref_multi" (
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
CREATE INDEX "idx_xref_multi_confidence" ON "fed_data"."entity_xref_multi" ("confidence" DESC);
-- Create index "idx_xref_multi_match_type" to table: "entity_xref_multi"
CREATE INDEX "idx_xref_multi_match_type" ON "fed_data"."entity_xref_multi" ("match_type");
-- Create index "idx_xref_multi_pair" to table: "entity_xref_multi"
CREATE UNIQUE INDEX "idx_xref_multi_pair" ON "fed_data"."entity_xref_multi" ("source_dataset", "source_id", "target_dataset", "target_id");
-- Create index "idx_xref_multi_source" to table: "entity_xref_multi"
CREATE INDEX "idx_xref_multi_source" ON "fed_data"."entity_xref_multi" ("source_dataset", "source_id");
-- Create index "idx_xref_multi_target" to table: "entity_xref_multi"
CREATE INDEX "idx_xref_multi_target" ON "fed_data"."entity_xref_multi" ("target_dataset", "target_id");

-- Create "ncen_registrants" table
CREATE TABLE "fed_data"."ncen_registrants" (
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
CREATE INDEX "idx_ncen_registrants_cik" ON "fed_data"."ncen_registrants" ("cik");

-- Create "ncen_funds" table
CREATE TABLE "fed_data"."ncen_funds" (
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
CREATE INDEX "idx_ncen_funds_accession" ON "fed_data"."ncen_funds" ("accession_number");
-- Create index "idx_ncen_funds_series" to table: "ncen_funds"
CREATE INDEX "idx_ncen_funds_series" ON "fed_data"."ncen_funds" ("series_id");

-- Create "ncen_advisers" table
CREATE TABLE "fed_data"."ncen_advisers" (
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
CREATE INDEX "idx_ncen_advisers_crd" ON "fed_data"."ncen_advisers" ("adviser_crd");
-- Create index "idx_ncen_advisers_fund" to table: "ncen_advisers"
CREATE INDEX "idx_ncen_advisers_fund" ON "fed_data"."ncen_advisers" ("fund_id");
