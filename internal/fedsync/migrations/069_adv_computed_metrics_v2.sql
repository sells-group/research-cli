-- Extend computed metrics with growth CAGR, concentration, key-person, profitability, amendment, and regulatory fields.
ALTER TABLE fed_data.adv_computed_metrics
    ADD COLUMN IF NOT EXISTS aum_1yr_growth_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS aum_3yr_cagr_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS aum_5yr_cagr_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS client_3yr_cagr_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS employee_3yr_cagr_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS concentration_risk_score SMALLINT,
    ADD COLUMN IF NOT EXISTS key_person_dependency_score SMALLINT,
    ADD COLUMN IF NOT EXISTS hybrid_revenue_estimate BIGINT,
    ADD COLUMN IF NOT EXISTS estimated_expense_ratio NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS estimated_operating_margin NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS revenue_per_employee BIGINT,
    ADD COLUMN IF NOT EXISTS benchmark_aum_per_employee_pctile NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS benchmark_fee_rate_pctile NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS amendments_last_year INTEGER,
    ADD COLUMN IF NOT EXISTS amendments_per_year_avg NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS has_frequent_amendments BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS regulatory_risk_score SMALLINT;
