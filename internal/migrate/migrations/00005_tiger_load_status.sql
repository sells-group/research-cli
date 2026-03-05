-- +goose Up
-- TIGER/Line parent tables and load status tracking for the tigerload command.
-- Parent tables are needed before per-state child tables can inherit from them.
-- All data columns are TEXT (shapefile string values); geometry is WGS84 (SRID 4326).

-- National parent tables.
CREATE TABLE IF NOT EXISTS tiger_data.state_all (
    region TEXT, division TEXT, statefp TEXT, statens TEXT, geoid TEXT, stusps TEXT,
    name TEXT, lsad TEXT, mtfcc TEXT, funcstat TEXT, aland TEXT, awater TEXT,
    intptlat TEXT, intptlon TEXT,
    the_geom geometry(MultiPolygon, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.county_all (
    statefp TEXT, countyfp TEXT, countyns TEXT, geoid TEXT, name TEXT, namelsad TEXT,
    lsad TEXT, classfp TEXT, mtfcc TEXT, csafp TEXT, cbsafp TEXT, metdivfp TEXT,
    funcstat TEXT, aland TEXT, awater TEXT, intptlat TEXT, intptlon TEXT,
    the_geom geometry(MultiPolygon, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.place (
    statefp TEXT, placefp TEXT, placens TEXT, geoid TEXT, name TEXT, namelsad TEXT,
    lsad TEXT, classfp TEXT, pcicbsa TEXT, pcinecta TEXT, mtfcc TEXT, funcstat TEXT,
    aland TEXT, awater TEXT, intptlat TEXT, intptlon TEXT,
    the_geom geometry(MultiPolygon, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.cousub (
    statefp TEXT, countyfp TEXT, cousubfp TEXT, cousubns TEXT, geoid TEXT, name TEXT,
    namelsad TEXT, lsad TEXT, classfp TEXT, mtfcc TEXT, cnectafp TEXT, nectafp TEXT,
    nctadvfp TEXT, funcstat TEXT, aland TEXT, awater TEXT, intptlat TEXT, intptlon TEXT,
    the_geom geometry(MultiPolygon, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.zcta5 (
    zcta5ce20 TEXT, geoid20 TEXT, classfp20 TEXT, mtfcc20 TEXT, funcstat20 TEXT,
    aland20 TEXT, awater20 TEXT, intptlat20 TEXT, intptlon20 TEXT,
    the_geom geometry(MultiPolygon, 4326)
);

-- Per-state parent tables (child tables inherit from these).
CREATE TABLE IF NOT EXISTS tiger_data.edges (
    tlid TEXT, statefp TEXT, countyfp TEXT, fullname TEXT, smtyp TEXT, mtfcc TEXT,
    lwflag TEXT, offsetl TEXT, offsetr TEXT, tfidl TEXT, tfidr TEXT, zipl TEXT, zipr TEXT,
    the_geom geometry(MultiLineString, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.faces (
    tfid TEXT, statefp00 TEXT, countyfp00 TEXT, tractce00 TEXT, blkgrpce00 TEXT,
    blockce00 TEXT, cousubfp00 TEXT, submcdfp00 TEXT, conctyfp00 TEXT, placefp00 TEXT,
    aiession00 TEXT, comptyp00 TEXT, cpi00 TEXT, statefp TEXT, countyfp TEXT,
    tractce TEXT, blkgrpce TEXT, blockce TEXT, cousubfp TEXT, submcdfp TEXT,
    conctyfp TEXT, placefp TEXT, aiession TEXT, comptyp TEXT, cpi TEXT, lwflag TEXT,
    the_geom geometry(MultiPolygon, 4326)
);
CREATE TABLE IF NOT EXISTS tiger_data.addr (
    tlid TEXT, fromhn TEXT, tohn TEXT, side TEXT, zip TEXT, plus4 TEXT, fromtyp TEXT,
    totyp TEXT, fromarmid TEXT, toarmid TEXT, aodo TEXT, statefp TEXT
);
CREATE TABLE IF NOT EXISTS tiger_data.featnames (
    tlid TEXT, fullname TEXT, name TEXT, predirabrv TEXT, pretypabrv TEXT,
    prequalabr TEXT, sufdirabrv TEXT, suftypabrv TEXT, sufqualabr TEXT, predir TEXT,
    pretyp TEXT, prequal TEXT, sufdir TEXT, suftyp TEXT, sufqual TEXT, linearid TEXT,
    mtfcc TEXT, paflag TEXT, statefp TEXT
);

-- Spatial indexes on parent tables.
CREATE INDEX IF NOT EXISTS idx_state_all_the_geom ON tiger_data.state_all USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_county_all_the_geom ON tiger_data.county_all USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_place_the_geom ON tiger_data.place USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_cousub_the_geom ON tiger_data.cousub USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_zcta5_the_geom ON tiger_data.zcta5 USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_edges_the_geom ON tiger_data.edges USING GIST (the_geom);
CREATE INDEX IF NOT EXISTS idx_faces_the_geom ON tiger_data.faces USING GIST (the_geom);

-- Load status tracking.
CREATE TABLE IF NOT EXISTS tiger_data.load_status (
    state_fips  TEXT NOT NULL,
    state_abbr  TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    year        INTEGER NOT NULL,
    row_count   INTEGER NOT NULL DEFAULT 0,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INTEGER,
    PRIMARY KEY (state_fips, table_name, year)
);

-- +goose Down
DROP TABLE IF EXISTS tiger_data.load_status;
DROP TABLE IF EXISTS tiger_data.featnames;
DROP TABLE IF EXISTS tiger_data.addr;
DROP TABLE IF EXISTS tiger_data.faces;
DROP TABLE IF EXISTS tiger_data.edges;
DROP TABLE IF EXISTS tiger_data.zcta5;
DROP TABLE IF EXISTS tiger_data.cousub;
DROP TABLE IF EXISTS tiger_data.place;
DROP TABLE IF EXISTS tiger_data.county_all;
DROP TABLE IF EXISTS tiger_data.state_all;
