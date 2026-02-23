-- Cross-advisor analytics views for relationship data.

CREATE OR REPLACE VIEW fed_data.v_custodian_advisors AS
SELECT
    cr.custodian_name,
    cr.crd_number,
    f.firm_name,
    fi.aum_total,
    fi.num_accounts,
    f.state
FROM fed_data.adv_custodian_relationships cr
JOIN fed_data.adv_firms f ON f.crd_number = cr.crd_number
LEFT JOIN LATERAL (
    SELECT aum_total, num_accounts
    FROM fed_data.adv_filings fi2
    WHERE fi2.crd_number = cr.crd_number
    ORDER BY fi2.filing_date DESC
    LIMIT 1
) fi ON true;

CREATE OR REPLACE VIEW fed_data.v_custodian_market_share AS
SELECT
    cr.custodian_name,
    COUNT(DISTINCT cr.crd_number) AS advisor_count,
    SUM(fi.aum_total) AS total_aum,
    AVG(fi.aum_total) AS avg_aum
FROM fed_data.adv_custodian_relationships cr
LEFT JOIN LATERAL (
    SELECT aum_total
    FROM fed_data.adv_filings fi2
    WHERE fi2.crd_number = cr.crd_number
    ORDER BY fi2.filing_date DESC
    LIMIT 1
) fi ON true
GROUP BY cr.custodian_name;

CREATE OR REPLACE VIEW fed_data.v_service_provider_network AS
SELECT
    sp.provider_name,
    sp.provider_type,
    COUNT(DISTINCT sp.crd_number) AS advisor_count,
    ARRAY_AGG(DISTINCT f.firm_name ORDER BY f.firm_name) AS advisor_names
FROM fed_data.adv_service_providers sp
JOIN fed_data.adv_firms f ON f.crd_number = sp.crd_number
GROUP BY sp.provider_name, sp.provider_type;
