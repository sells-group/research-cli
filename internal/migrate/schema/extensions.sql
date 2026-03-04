-- schema/extensions.sql
-- Required PostgreSQL extensions

CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "postgis_tiger_geocoder";
CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch";
