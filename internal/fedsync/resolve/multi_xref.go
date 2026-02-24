package resolve

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// MultiXrefBuilder builds cross-references across all entity-bearing federal
// datasets using multiple match strategies: direct CRD, direct CIK, exact
// name+zip, exact name+state, and fuzzy name+state.
type MultiXrefBuilder struct {
	pool db.Pool
}

// NewMultiXrefBuilder creates a new MultiXrefBuilder.
func NewMultiXrefBuilder(pool db.Pool) *MultiXrefBuilder {
	return &MultiXrefBuilder{pool: pool}
}

// passSpec describes a single cross-reference match pass.
type passSpec struct {
	name string
	sql  string
}

// Build executes all match passes and rebuilds the entity_xref_multi table.
// Returns total matched rows and per-pass counts.
func (m *MultiXrefBuilder) Build(ctx context.Context) (int64, map[string]int64, error) {
	log := zap.L().With(zap.String("component", "multi_xref_builder"))

	if _, err := m.pool.Exec(ctx, "TRUNCATE TABLE fed_data.entity_xref_multi"); err != nil {
		return 0, nil, eris.Wrap(err, "multi_xref: truncate entity_xref_multi")
	}

	passes := allPasses()
	var total int64
	counts := make(map[string]int64, len(passes))

	for i, p := range passes {
		log.Info(fmt.Sprintf("multi_xref pass %d/%d: %s", i+1, len(passes), p.name))

		tag, err := m.pool.Exec(ctx, p.sql)
		if err != nil {
			return total, counts, eris.Wrapf(err, "multi_xref: pass %s", p.name)
		}

		n := tag.RowsAffected()
		counts[p.name] = n
		total += n
		log.Info(fmt.Sprintf("multi_xref pass %s complete", p.name), zap.Int64("matched", n))
	}

	return total, counts, nil
}

// allPasses returns the ordered list of cross-reference match passes.
func allPasses() []passSpec {
	normName := NormalizeNameSQL

	return []passSpec{
		// --- Pass group 1: Direct CRD linkage (confidence 1.0) ---
		{
			name: "crd_adv_brokercheck",
			sql:  directCRDSQL("adv_firms", "brokercheck", "a.firm_name"),
		},
		{
			name: "crd_adv_form_bd",
			sql:  directCRDSQL("adv_firms", "form_bd", "a.firm_name"),
		},

		// --- Pass group 2: Direct CIK linkage (confidence 1.0) ---
		{
			name: "cik_adv_edgar",
			sql:  cikAdvEdgarSQL(),
		},
		{
			name: "cik_form_d_edgar",
			sql:  cikFormDEdgarSQL(),
		},

		// --- Pass group 3: Exact name + zip (confidence 0.92) ---
		{
			name: "name_zip_fpds_ppp",
			sql: exactNameGeoSQL(
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.92, normName,
			),
		},
		{
			name: "name_zip_osha_epa",
			sql: exactNameGeoSQL(
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fpds_osha",
			sql: exactNameGeoSQL(
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fpds_epa",
			sql: exactNameGeoSQL(
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// --- Pass group 4: Exact name + state (confidence 0.88) ---
		{
			name: "name_state_adv_osha",
			sql: exactNameGeoSQL(
				"adv_firms", "crd_number", "firm_name", "state",
				"osha_inspections", "activity_nr", "estab_name", "site_state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_adv_epa",
			sql: exactNameGeoSQL(
				"adv_firms", "crd_number", "firm_name", "state",
				"epa_facilities", "registry_id", "fac_name", "fac_state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_adv_fpds",
			sql: exactNameGeoSQL(
				"adv_firms", "crd_number", "firm_name", "state",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_adv_ppp",
			sql: exactNameGeoSQL(
				"adv_firms", "crd_number", "firm_name", "state",
				"ppp_loans", "loannumber", "borrowername", "borrowerstate",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_edgar_fpds",
			sql: exactNameGeoSQL(
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_state",
				"state", 0.88, normName,
			),
		},

		// --- Pass group 5: Fuzzy name + state (confidence 0.60-0.90) ---
		{
			name: "fuzzy_adv_ppp",
			sql: fuzzyNameStateSQL(
				"adv_firms", "crd_number", "firm_name", "state",
				"ppp_loans", "loannumber", "borrowername", "borrowerstate",
			),
		},
		{
			name: "fuzzy_edgar_fpds",
			sql: fuzzyNameStateSQL(
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_state",
			),
		},
	}
}

// directCRDSQL generates SQL for direct CRD matching between two tables
// that both have a crd_number column.
func directCRDSQL(srcTable, tgtTable, nameCol string) string {
	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT
    '%[1]s',
    a.crd_number::TEXT,
    '%[2]s',
    b.crd_number::TEXT,
    %[3]s,
    'direct_crd',
    1.00
FROM fed_data.%[1]s a
JOIN fed_data.%[2]s b ON a.crd_number = b.crd_number
WHERE a.crd_number IS NOT NULL
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		srcTable, tgtTable, nameCol)
}

// cikAdvEdgarSQL generates SQL for CRD→CIK cross-linking via the ADV sec_number field.
func cikAdvEdgarSQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT
    'adv_firms',
    a.crd_number::TEXT,
    'edgar_entities',
    e.cik,
    a.firm_name,
    'direct_cik',
    1.00
FROM fed_data.adv_firms a
JOIN fed_data.edgar_entities e
    ON LPAD(REPLACE(a.sec_number, '-', ''), 10, '0') = e.cik
WHERE a.sec_number IS NOT NULL
  AND a.sec_number != ''
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// cikFormDEdgarSQL generates SQL for Form D → EDGAR direct CIK matching.
func cikFormDEdgarSQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (f.cik)
    'form_d',
    f.accession_number,
    'edgar_entities',
    e.cik,
    f.entity_name,
    'direct_cik',
    1.00
FROM fed_data.form_d f
JOIN fed_data.edgar_entities e ON f.cik = e.cik
WHERE f.cik IS NOT NULL
  AND f.cik != ''
ORDER BY f.cik, f.filing_date DESC NULLS LAST
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// exactNameGeoSQL generates SQL for exact normalized-name + geographic match.
// geoType is "zip" or "state" to indicate which geographic column to match on.
func exactNameGeoSQL(
	srcTable, srcPK, srcName, srcGeo,
	tgtTable, tgtPK, tgtName, tgtGeo,
	geoType string, confidence float64,
	normFn func(string) string,
) string {
	matchType := fmt.Sprintf("exact_name_%s", geoType)

	// Use LEFT(x,5) for zip matching to handle zip+4 codes.
	geoJoin := fmt.Sprintf("a.%s = b.%s", srcGeo, tgtGeo)
	if geoType == "zip" {
		geoJoin = fmt.Sprintf("LEFT(a.%s, 5) = LEFT(b.%s, 5)", srcGeo, tgtGeo)
	}

	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.%[3]s::TEXT, b.%[7]s::TEXT)
    '%[1]s',
    a.%[3]s::TEXT,
    '%[5]s',
    b.%[7]s::TEXT,
    a.%[4]s,
    '%[10]s',
    %[11]v
FROM fed_data.%[1]s a
JOIN fed_data.%[5]s b
    ON %[9]s = %[12]s
    AND %[13]s
WHERE a.%[4]s IS NOT NULL AND a.%[4]s != ''
  AND b.%[8]s IS NOT NULL AND b.%[8]s != ''
  AND a.%[14]s IS NOT NULL AND a.%[14]s != ''
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref_multi x
      WHERE x.source_dataset = '%[1]s' AND x.source_id = a.%[3]s::TEXT
        AND x.target_dataset = '%[5]s' AND x.target_id = b.%[7]s::TEXT
  )
ORDER BY a.%[3]s::TEXT, b.%[7]s::TEXT
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		srcTable,             // 1
		"",                   // 2 (unused)
		srcPK,                // 3
		srcName,              // 4
		tgtTable,             // 5
		"",                   // 6 (unused)
		tgtPK,                // 7
		tgtName,              // 8
		normFn("a."+srcName), // 9
		matchType,            // 10
		confidence,           // 11
		normFn("b."+tgtName), // 12
		geoJoin,              // 13
		srcGeo,               // 14
	)
}

// fuzzyNameStateSQL generates SQL for fuzzy name matching with state constraint.
// Uses pg_trgm similarity > 0.6, with confidence set to the similarity score.
func fuzzyNameStateSQL(
	srcTable, srcPK, srcName, srcState,
	tgtTable, tgtPK, tgtName, tgtState string,
) string {
	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.%[3]s::TEXT)
    '%[1]s',
    a.%[3]s::TEXT,
    '%[5]s',
    b.%[7]s::TEXT,
    a.%[4]s,
    'fuzzy_name_state',
    similarity(UPPER(a.%[4]s), UPPER(b.%[8]s))::NUMERIC(3,2)
FROM fed_data.%[1]s a
JOIN fed_data.%[5]s b
    ON similarity(UPPER(a.%[4]s), UPPER(b.%[8]s)) > 0.6
    AND a.%[9]s = b.%[10]s
WHERE a.%[4]s IS NOT NULL AND a.%[4]s != ''
  AND b.%[8]s IS NOT NULL AND b.%[8]s != ''
  AND a.%[9]s IS NOT NULL AND a.%[9]s != ''
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref_multi x
      WHERE x.source_dataset = '%[1]s' AND x.source_id = a.%[3]s::TEXT
        AND x.target_dataset = '%[5]s'
  )
ORDER BY a.%[3]s::TEXT, similarity(UPPER(a.%[4]s), UPPER(b.%[8]s)) DESC
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		srcTable, // 1
		"",       // 2 (unused)
		srcPK,    // 3
		srcName,  // 4
		tgtTable, // 5
		"",       // 6 (unused)
		tgtPK,    // 7
		tgtName,  // 8
		srcState, // 9
		tgtState, // 10
	)
}
