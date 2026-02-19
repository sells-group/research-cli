-- 050: Extend adv_firm_details with Items 2, 8, 9, and 11 from FOIA CSV.
-- These are high-value columns previously unparsed from the ~448-column FOIA roster.

-- Item 2: Registration status
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS sec_registered BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS exempt_reporting BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS state_registered BOOLEAN DEFAULT false;

-- Item 5L: Discretionary authority
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS discretionary_authority BOOLEAN DEFAULT false;

-- Item 8: Participation or interest in client transactions
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_proprietary_interest BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_sells_own_securities BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_buys_from_clients BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_recommends_own BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_recommends_broker BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_agency_cross BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_principal BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_referral_compensation BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_other_research BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS txn_revenue_sharing BOOLEAN DEFAULT false;

-- Item 9: Custody arrangements
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS custody_client_cash BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS custody_client_securities BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS custody_related_person BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS custody_qualified_custodian BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS custody_surprise_exam BOOLEAN DEFAULT false;

-- Item 11: DRP (Disciplinary Reporting Page) trigger flags
-- These indicate TYPES of disclosures; actual narratives are in adv_disclosures.
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_criminal_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_criminal_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_regulatory_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_regulatory_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_civil_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_civil_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_complaint_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_complaint_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_termination_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_termination_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_judgment BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_financial_firm BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS drp_financial_affiliate BOOLEAN DEFAULT false;
ALTER TABLE fed_data.adv_firm_details ADD COLUMN IF NOT EXISTS has_any_drp BOOLEAN DEFAULT false;

-- Index on DRP for due diligence queries
CREATE INDEX IF NOT EXISTS idx_firm_details_drp
    ON fed_data.adv_firm_details (has_any_drp) WHERE has_any_drp = true;

-- Index on custody for compliance queries
CREATE INDEX IF NOT EXISTS idx_firm_details_custody
    ON fed_data.adv_firm_details (custody_client_cash, custody_client_securities);

-- Index on registration status
CREATE INDEX IF NOT EXISTS idx_firm_details_registration
    ON fed_data.adv_firm_details (sec_registered, exempt_reporting);
