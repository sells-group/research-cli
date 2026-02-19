-- Widen EPA facility columns to accommodate FRS national data.
-- fac_zip: some international/extended codes exceed 10 chars.
-- fac_state: some FRS records use longer state identifiers.
ALTER TABLE fed_data.epa_facilities ALTER COLUMN fac_zip TYPE varchar(20);
ALTER TABLE fed_data.epa_facilities ALTER COLUMN fac_state TYPE varchar(10);
