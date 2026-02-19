-- Drop materialized view that depends on adv_firms.state; rebuilt in 051.
DROP MATERIALIZED VIEW IF EXISTS fed_data.mv_firm_combined;

ALTER TABLE fed_data.adv_firms ALTER COLUMN state TYPE VARCHAR(10);
