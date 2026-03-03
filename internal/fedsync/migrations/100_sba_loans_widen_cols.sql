-- Widen constrained columns to TEXT to handle real-world data variations
-- (leading spaces, non-standard state codes, multi-char indicators).

ALTER TABLE fed_data.sba_loans ALTER COLUMN borrstate TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN projectstate TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN bankstate TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN cdc_state TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN thirdpartylender_state TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN fixedorvariableinterestind TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN naicscode TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN collateralind TYPE TEXT;
ALTER TABLE fed_data.sba_loans ALTER COLUMN soldsecmrktind TYPE TEXT;
