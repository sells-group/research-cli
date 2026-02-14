-- 001_create_schema.sql: Create the fed_data schema for federal dataset tables.
CREATE SCHEMA IF NOT EXISTS fed_data;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
