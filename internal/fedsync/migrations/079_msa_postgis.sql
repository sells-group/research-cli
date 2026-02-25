CREATE EXTENSION IF NOT EXISTS postgis;

-- CBSA (Core Based Statistical Area) polygons from Census Bureau shapefiles
CREATE TABLE IF NOT EXISTS public.cbsa_areas (
    gid         SERIAL PRIMARY KEY,
    cbsa_code   VARCHAR(5) NOT NULL UNIQUE,
    name        VARCHAR(200) NOT NULL,
    lsad        VARCHAR(2),          -- 'M1' (metro) or 'M2' (micro)
    geom        geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX idx_cbsa_geom ON public.cbsa_areas USING GIST (geom);
CREATE INDEX idx_cbsa_code ON public.cbsa_areas (cbsa_code);

-- Pre-computed grid cells for target MSAs (populated by discover grid-gen)
CREATE TABLE IF NOT EXISTS public.msa_grid_cells (
    id          BIGSERIAL PRIMARY KEY,
    cbsa_code   VARCHAR(5) NOT NULL REFERENCES public.cbsa_areas(cbsa_code),
    cell_km     NUMERIC(4,1) NOT NULL,
    sw_lat      NUMERIC(9,6) NOT NULL,
    sw_lon      NUMERIC(9,6) NOT NULL,
    ne_lat      NUMERIC(9,6) NOT NULL,
    ne_lon      NUMERIC(9,6) NOT NULL,
    geom        geometry(Polygon, 4326) NOT NULL,
    searched_at TIMESTAMPTZ,          -- track which cells have been searched
    result_count INTEGER,
    UNIQUE (cbsa_code, cell_km, sw_lat, sw_lon)
);

CREATE INDEX idx_grid_cbsa ON public.msa_grid_cells (cbsa_code, cell_km);
CREATE INDEX idx_grid_geom ON public.msa_grid_cells USING GIST (geom);
CREATE INDEX idx_grid_unsearched ON public.msa_grid_cells (cbsa_code) WHERE searched_at IS NULL;
