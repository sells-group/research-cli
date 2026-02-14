package resolve

// Pass1DirectSQL returns the SQL for pass 1: direct CRD-CIK matching.
// Matches ADV firms to EDGAR entities where the ADV sec_number corresponds
// to an EDGAR CIK (with leading-zero padding).
func Pass1DirectSQL() string {
	return `
INSERT INTO fed_data.entity_xref (crd_number, cik, entity_name, match_type, confidence)
SELECT
    a.crd_number,
    e.cik,
    a.firm_name,
    'direct_sec_number',
    1.00
FROM fed_data.adv_firms a
JOIN fed_data.edgar_entities e
    ON LPAD(REPLACE(a.sec_number, '-', ''), 10, '0') = e.cik
WHERE a.sec_number IS NOT NULL
  AND a.sec_number != ''
ON CONFLICT (crd_number, cik) WHERE crd_number IS NOT NULL AND cik IS NOT NULL
DO NOTHING`
}

// Pass2SICSQL returns the SQL for pass 2: SIC code based exact name matching.
// Matches ADV firms to EDGAR entities that have investment advisor SIC codes
// (6211 = Security Brokers/Dealers, 6282 = Investment Advice) by exact name.
func Pass2SICSQL() string {
	return `
INSERT INTO fed_data.entity_xref (crd_number, cik, entity_name, match_type, confidence)
SELECT
    a.crd_number,
    e.cik,
    a.firm_name,
    'sic_exact_name',
    0.95
FROM fed_data.adv_firms a
JOIN fed_data.edgar_entities e
    ON UPPER(TRIM(a.firm_name)) = UPPER(TRIM(e.entity_name))
WHERE e.sic IN ('6211', '6282')
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref x
      WHERE x.crd_number = a.crd_number AND x.cik = e.cik
  )
ON CONFLICT (crd_number, cik) WHERE crd_number IS NOT NULL AND cik IS NOT NULL
DO NOTHING`
}

// FuzzyMatchSQL returns the SQL for pass 3: fuzzy name matching using pg_trgm.
// Matches ADV firms to EDGAR entities in financial services SIC codes using
// trigram similarity with a threshold of 0.6.
func FuzzyMatchSQL() string {
	return `
INSERT INTO fed_data.entity_xref (crd_number, cik, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.crd_number)
    a.crd_number,
    e.cik,
    a.firm_name,
    'fuzzy_name',
    similarity(UPPER(a.firm_name), UPPER(e.entity_name))::NUMERIC(3,2)
FROM fed_data.adv_firms a
JOIN fed_data.edgar_entities e
    ON similarity(UPPER(a.firm_name), UPPER(e.entity_name)) > 0.6
WHERE e.sic IN ('6211', '6282', '6199', '6726', '6159')
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref x
      WHERE x.crd_number = a.crd_number
  )
ORDER BY a.crd_number, similarity(UPPER(a.firm_name), UPPER(e.entity_name)) DESC
ON CONFLICT (crd_number, cik) WHERE crd_number IS NOT NULL AND cik IS NOT NULL
DO NOTHING`
}
