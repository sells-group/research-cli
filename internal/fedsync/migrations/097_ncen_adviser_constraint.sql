-- Add unique constraint on ncen_advisers so ON CONFLICT upserts work.
-- Natural key: (fund_id, adviser_name, adviser_type) since the same adviser
-- can serve a fund in different roles (e.g., adviser vs sub-adviser).

-- Backfill NULLs with empty strings before adding NOT NULL constraints.
UPDATE fed_data.ncen_advisers SET adviser_name = '' WHERE adviser_name IS NULL;
UPDATE fed_data.ncen_advisers SET adviser_type = '' WHERE adviser_type IS NULL;

ALTER TABLE fed_data.ncen_advisers
    ALTER COLUMN adviser_name SET NOT NULL,
    ALTER COLUMN adviser_name SET DEFAULT '',
    ALTER COLUMN adviser_type SET NOT NULL,
    ALTER COLUMN adviser_type SET DEFAULT '';

ALTER TABLE fed_data.ncen_advisers
    ADD CONSTRAINT ncen_advisers_pkey PRIMARY KEY (fund_id, adviser_name, adviser_type);
