-- 008: Materialized views joining fed_data tables with geo boundaries.
-- These cross-reference views enable spatial analysis of federal datasets.

-- County-level economic indicators (CBP + QCEW joined to geo.counties).
-- CBP: total-industry rows (naics = '------'), QCEW: private ownership (own_code = '5'),
-- latest quarter per area/year.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_county_economics AS
SELECT
    c.geoid,
    c.name                  AS county_name,
    c.state_fips,
    cbp.year,
    cbp.est                 AS cbp_establishments,
    cbp.emp                 AS cbp_employment,
    cbp.ap                  AS cbp_annual_payroll,
    qcew.total_qtrly_wages  AS qcew_quarterly_wages,
    qcew.month1_emplvl      AS qcew_employment
FROM geo.counties c
LEFT JOIN fed_data.cbp_data cbp
    ON c.state_fips = cbp.fips_state
   AND c.county_fips = cbp.fips_county
   AND cbp.naics = '------'
LEFT JOIN LATERAL (
    SELECT q.total_qtrly_wages, q.month1_emplvl
    FROM fed_data.qcew_data q
    WHERE q.area_fips = c.state_fips || c.county_fips
      AND q.own_code = '5'
      AND q.industry_code = '10'
      AND q.year = cbp.year
    ORDER BY q.qtr DESC
    LIMIT 1
) qcew ON true
WHERE cbp.year IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_county_economics_pk
    ON geo.mv_county_economics (geoid, year);

-- CBSA-level demographics summary.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_cbsa_summary AS
SELECT
    cb.cbsa_code,
    cb.name     AS cbsa_name,
    cb.lsad,
    d.total_population,
    d.median_income,
    d.median_age,
    d.housing_units,
    d.year      AS demo_year
FROM geo.cbsa cb
LEFT JOIN geo.demographics d
    ON cb.cbsa_code = d.geoid
   AND d.geo_level = 'cbsa';

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cbsa_summary_pk
    ON geo.mv_cbsa_summary (cbsa_code, demo_year);

-- EPA facility locations matched to counties via spatial containment.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_epa_by_county AS
SELECT
    c.geoid             AS county_geoid,
    c.name              AS county_name,
    c.state_fips,
    COUNT(e.id)         AS facility_count,
    ARRAY_AGG(DISTINCT e.program) AS programs
FROM geo.counties c
JOIN geo.epa_sites e ON ST_Contains(c.geom, e.geom)
GROUP BY c.geoid, c.name, c.state_fips;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_epa_by_county_pk
    ON geo.mv_epa_by_county (county_geoid);
CREATE INDEX IF NOT EXISTS idx_mv_epa_by_county_state
    ON geo.mv_epa_by_county (state_fips);

-- Infrastructure density by county and type via spatial containment.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_infrastructure_by_county AS
SELECT
    c.geoid             AS county_geoid,
    c.name              AS county_name,
    c.state_fips,
    i.type              AS infrastructure_type,
    COUNT(i.id)         AS count,
    SUM(i.capacity)     AS total_capacity
FROM geo.counties c
JOIN geo.infrastructure i ON ST_Contains(c.geom, i.geom)
GROUP BY c.geoid, c.name, c.state_fips, i.type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_infrastructure_by_county_pk
    ON geo.mv_infrastructure_by_county (county_geoid, infrastructure_type);
CREATE INDEX IF NOT EXISTS idx_mv_infrastructure_by_county_type
    ON geo.mv_infrastructure_by_county (infrastructure_type);

-- ADV firms by state (aggregated from fed_data.adv_firms).
-- adv_firms lacks a cbsa_code column, so we aggregate by state/city.
CREATE MATERIALIZED VIEW IF NOT EXISTS geo.mv_adv_firms_by_state AS
SELECT
    af.state,
    COUNT(af.crd_number)        AS firm_count,
    SUM(af.aum)                 AS total_aum,
    SUM(af.num_employees)       AS total_employees,
    SUM(af.num_accounts)        AS total_accounts
FROM fed_data.adv_firms af
WHERE af.state IS NOT NULL
GROUP BY af.state;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_adv_firms_by_state_pk
    ON geo.mv_adv_firms_by_state (state);
