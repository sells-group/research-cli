-- 089: Add street address columns to adv_firms for geocoding accuracy.
-- SEC ADV FOIA Item 1F includes street address lines which significantly
-- improve PostGIS TIGER geocode accuracy vs city/state only.
ALTER TABLE fed_data.adv_firms ADD COLUMN IF NOT EXISTS street1 VARCHAR(200);
ALTER TABLE fed_data.adv_firms ADD COLUMN IF NOT EXISTS street2 VARCHAR(200);
ALTER TABLE fed_data.adv_firms ADD COLUMN IF NOT EXISTS zip VARCHAR(10);
