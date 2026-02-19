-- Widen epa_facilities.registry_id from varchar(20) to varchar(50).
-- The new EPA FRS data source (national_single.zip) uses longer registry IDs.
ALTER TABLE fed_data.epa_facilities ALTER COLUMN registry_id TYPE varchar(50);
