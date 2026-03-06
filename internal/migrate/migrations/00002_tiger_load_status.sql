-- +goose Up
-- Create load_status table if it doesn't exist.
CREATE TABLE IF NOT EXISTS tiger_data.load_status (
    state_fips varchar(2) NOT NULL,
    state_abbr varchar(2) NOT NULL,
    table_name varchar(50) NOT NULL,
    year integer NOT NULL,
    row_count integer NOT NULL DEFAULT 0,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    duration_ms integer,
    PRIMARY KEY (state_fips, table_name, year)
);

-- Fix tables created manually with a serial id PK instead of the composite PK.
-- Drop the id column and old PK, then add the correct composite PK.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'tiger_data' AND table_name = 'load_status' AND column_name = 'id'
    ) THEN
        -- Remove old serial PK.
        ALTER TABLE tiger_data.load_status DROP CONSTRAINT IF EXISTS load_status_pkey;
        ALTER TABLE tiger_data.load_status DROP COLUMN id;
        -- Add composite PK (safe because no duplicates on state_fips+table_name+year).
        ALTER TABLE tiger_data.load_status ADD PRIMARY KEY (state_fips, table_name, year);
    END IF;
END $$;

-- +goose Down
DROP TABLE IF EXISTS tiger_data.load_status;
