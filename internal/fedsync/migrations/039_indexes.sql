-- 039_indexes.sql: Cross-dataset indexes for common join patterns.

-- ADV firms → 13F filers linkage via entity xref
CREATE INDEX IF NOT EXISTS idx_entity_xref_match ON fed_data.entity_xref (match_type, confidence DESC);

-- FPDS vendor to ADV firm matching
CREATE INDEX IF NOT EXISTS idx_fpds_vendor_state ON fed_data.fpds_contracts (vendor_state);

-- QCEW area + industry for cross-referencing with CBP
CREATE INDEX IF NOT EXISTS idx_qcew_area_industry ON fed_data.qcew_data (area_fips, industry_code);

-- EDGAR filings by date range for recent filing queries
CREATE INDEX IF NOT EXISTS idx_edgar_filings_date_form ON fed_data.edgar_filings (filing_date DESC, form_type);

-- XBRL facts by CIK + fact for financial lookups
CREATE INDEX IF NOT EXISTS idx_xbrl_cik_fact ON fed_data.xbrl_facts (cik, fact_name);

-- FRED series by date for time-series queries
CREATE INDEX IF NOT EXISTS idx_fred_date ON fed_data.fred_series (obs_date DESC);

-- BrokerCheck + ADV join on CRD
CREATE INDEX IF NOT EXISTS idx_brokercheck_crd ON fed_data.brokercheck (crd_number);
