-- Widen sponsor_zip to accommodate DOL data (some entries have 12-char ZIP codes).
ALTER TABLE fed_data.form_5500 ALTER COLUMN sponsor_zip TYPE VARCHAR(12);
