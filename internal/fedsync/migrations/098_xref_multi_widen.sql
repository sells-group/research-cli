-- Widen entity_xref_multi ID columns from VARCHAR(50) to TEXT.
-- USAspending award_id and other identifiers can exceed 50 characters.
ALTER TABLE fed_data.entity_xref_multi
    ALTER COLUMN source_id TYPE TEXT,
    ALTER COLUMN target_id TYPE TEXT;
