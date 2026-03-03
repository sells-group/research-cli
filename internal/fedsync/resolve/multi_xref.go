package resolve

import (
	"context"
	"fmt"
	"strings"

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

// AllPassSQL returns the concatenated SQL of all cross-reference passes.
// Used by CI tests to verify that every entity-bearing dataset has at least
// one xref pass covering its table.
func AllPassSQL() string {
	passes := allPasses()
	var sb strings.Builder
	for _, p := range passes {
		sb.WriteString(p.sql)
		sb.WriteByte('\n')
	}
	return sb.String()
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
		{
			name: "crd_ncen_adv",
			sql:  crdNCENAdvSQL(),
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
		{
			name: "cik_ncen_edgar",
			sql:  cikNCENEdgarSQL(),
		},

		// --- Pass group 3: Direct DUNS/UEI linkage (confidence 1.0) ---
		{
			name: "duns_usa_fpds",
			sql:  directDUNSSQL(),
		},
		{
			name: "uei_usa_fpds",
			sql:  directUEISQL(),
		},

		// --- Pass group 4: Direct EIN linkage (confidence 0.95) ---
		{
			name: "ein_5500_edgar",
			sql: directEINSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_ein",
				"edgar_entities", "cik", "entity_name", "ein",
			),
		},
		{
			name: "ein_eobmf_edgar",
			sql: directEINSQL(
				"eo_bmf", "ein", "name", "ein",
				"edgar_entities", "cik", "entity_name", "ein",
			),
		},
		{
			name: "ein_5500_eobmf",
			sql: directEINSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_ein",
				"eo_bmf", "ein", "name", "ein",
			),
		},

		// --- Pass group 4B: Direct FDIC cert linkage (confidence 0.95) ---
		{
			name: "fdic_sba_bank",
			sql:  directFDICSBASQL(),
		},

		// --- Pass group 5: Exact name + zip (confidence 0.90-0.92) ---
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

		// Form 5500 ↔ operational datasets
		{
			name: "name_zip_5500_fpds",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_5500_ppp",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_5500_osha",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_5500_epa",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// EO BMF ↔ operational datasets
		{
			name: "name_zip_eobmf_fpds",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_eobmf_ppp",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_eobmf_osha",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_eobmf_epa",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// FDIC ↔ operational datasets
		{
			name: "name_zip_fdic_fpds",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "zip",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fdic_ppp",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fdic_osha",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fdic_epa",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// USAspending ↔ operational datasets (+ FPDS fallback for missing DUNS/UEI)
		{
			name: "name_zip_usa_fpds",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_usa_ppp",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_usa_osha",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_usa_epa",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// N-CEN ↔ operational datasets (zip-based, complements existing state passes)
		{
			name: "name_zip_ncen_ppp",
			sql: exactNameGeoSQL(
				"ncen_registrants", "accession_number", "registrant_name", "zip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_ncen_osha",
			sql: exactNameGeoSQL(
				"ncen_registrants", "accession_number", "registrant_name", "zip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_ncen_epa",
			sql: exactNameGeoSQL(
				"ncen_registrants", "accession_number", "registrant_name", "zip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},

		// SBA 7(a)/504 ↔ operational datasets (name+zip)
		{
			name: "name_zip_sba_fpds",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_ppp",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"ppp_loans", "loannumber", "borrowername", "borrowerzip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_osha",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"osha_inspections", "activity_nr", "estab_name", "site_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_epa",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"epa_facilities", "registry_id", "fac_name", "fac_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_5500",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_eobmf",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"eo_bmf", "ein", "name", "zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_fdic",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"fdic_institutions", "cert", "name", "zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_usa",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_sba_ncen",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrzip",
				"ncen_registrants", "accession_number", "registrant_name", "zip",
				"zip", 0.90, normName,
			),
		},

		// Cross-dataset (new ↔ new, both have zip)
		{
			name: "name_zip_5500_fdic",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"fdic_institutions", "cert", "name", "zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_5500_usa",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_zip",
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_eobmf_fdic",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"fdic_institutions", "cert", "name", "zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_eobmf_usa",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "zip",
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"zip", 0.90, normName,
			),
		},
		{
			name: "name_zip_fdic_usa",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "zip",
				"usaspending_awards", "award_id", "recipient_name", "recipient_zip",
				"zip", 0.90, normName,
			),
		},

		// --- Pass group 6: Exact name + state (confidence 0.88) ---
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
		{
			name: "name_state_ncen_adv",
			sql:  nameStateNCENAdvSQL(normName),
		},
		{
			name: "name_state_ncen_fpds",
			sql:  nameStateNCENFpdsSQL(normName),
		},

		// Form 5500 ↔ hub datasets (no zip on ADV/EDGAR)
		{
			name: "name_state_5500_adv",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_state",
				"adv_firms", "crd_number", "firm_name", "state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_5500_edgar",
			sql: exactNameGeoSQL(
				"form_5500", "ack_id", "sponsor_dfe_name", "spons_dfe_mail_us_state",
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"state", 0.88, normName,
			),
		},

		// EO BMF ↔ hub datasets
		{
			name: "name_state_eobmf_adv",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "state",
				"adv_firms", "crd_number", "firm_name", "state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_eobmf_edgar",
			sql: exactNameGeoSQL(
				"eo_bmf", "ein", "name", "state",
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"state", 0.88, normName,
			),
		},

		// FDIC ↔ hub datasets (state column is "stalp")
		{
			name: "name_state_fdic_adv",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "stalp",
				"adv_firms", "crd_number", "firm_name", "state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_fdic_edgar",
			sql: exactNameGeoSQL(
				"fdic_institutions", "cert", "name", "stalp",
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"state", 0.88, normName,
			),
		},

		// USAspending ↔ hub datasets
		{
			name: "name_state_usa_adv",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_state",
				"adv_firms", "crd_number", "firm_name", "state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_usa_edgar",
			sql: exactNameGeoSQL(
				"usaspending_awards", "award_id", "recipient_name", "recipient_state",
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"state", 0.88, normName,
			),
		},

		// SBA 7(a)/504 ↔ hub datasets (name+state, no zip on ADV/EDGAR)
		{
			name: "name_state_sba_adv",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrstate",
				"adv_firms", "crd_number", "firm_name", "state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_sba_edgar",
			sql: exactNameGeoSQL(
				"sba_loans", "l2locid", "borrname", "borrstate",
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"state", 0.88, normName,
			),
		},

		// EDGAR ↔ remaining operational datasets (previously missing)
		{
			name: "name_state_edgar_ppp",
			sql: exactNameGeoSQL(
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"ppp_loans", "loannumber", "borrowername", "borrowerstate",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_edgar_osha",
			sql: exactNameGeoSQL(
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"osha_inspections", "activity_nr", "estab_name", "site_state",
				"state", 0.88, normName,
			),
		},
		{
			name: "name_state_edgar_epa",
			sql: exactNameGeoSQL(
				"edgar_entities", "cik", "entity_name", "state_of_business",
				"epa_facilities", "registry_id", "fac_name", "fac_state",
				"state", 0.88, normName,
			),
		},

		// --- Pass group 7: Fuzzy name + state (confidence 0.60-0.90) ---
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

// directFDICSBASQL generates SQL for matching SBA 7(a) loan bank FDIC numbers
// to FDIC institutions by certificate number.
func directFDICSBASQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.l2locid)
    'sba_loans',
    a.l2locid::TEXT,
    'fdic_institutions',
    b.cert::TEXT,
    a.borrname,
    'direct_fdic_cert',
    0.95
FROM fed_data.sba_loans a
JOIN fed_data.fdic_institutions b
    ON a.bankfdicnumber = b.cert::TEXT
WHERE a.program = '7A'
  AND a.bankfdicnumber IS NOT NULL AND a.bankfdicnumber != ''
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref_multi x
      WHERE x.source_dataset = 'sba_loans' AND x.source_id = a.l2locid::TEXT
        AND x.target_dataset = 'fdic_institutions' AND x.target_id = b.cert::TEXT
  )
ORDER BY a.l2locid, b.cert
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// directEINSQL generates SQL for direct EIN matching between two tables.
// Normalizes EINs by stripping dashes (EDGAR stores "XX-XXXXXXX", others store "XXXXXXXXX").
// Uses DISTINCT ON to deduplicate when a source table has multiple rows per EIN.
func directEINSQL(
	srcTable, srcPK, srcName, srcEIN,
	tgtTable, tgtPK, tgtName, tgtEIN string,
) string {
	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (REPLACE(a.%[4]s, '-', ''))
    '%[1]s',
    a.%[2]s::TEXT,
    '%[5]s',
    b.%[6]s::TEXT,
    a.%[3]s,
    'direct_ein',
    0.95
FROM fed_data.%[1]s a
JOIN fed_data.%[5]s b ON REPLACE(a.%[4]s, '-', '') = REPLACE(b.%[8]s, '-', '')
WHERE a.%[4]s IS NOT NULL AND a.%[4]s != ''
  AND b.%[8]s IS NOT NULL AND b.%[8]s != ''
ORDER BY REPLACE(a.%[4]s, '-', '')
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		srcTable, srcPK, srcName, srcEIN, // 1-4
		tgtTable, tgtPK, tgtName, tgtEIN, // 5-8
	)
}

// directDUNSSQL generates SQL for USAspending → FPDS direct DUNS matching.
func directDUNSSQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.recipient_duns)
    'usaspending_awards',
    a.award_id,
    'fpds_contracts',
    b.contract_id,
    a.recipient_name,
    'direct_duns',
    1.00
FROM fed_data.usaspending_awards a
JOIN fed_data.fpds_contracts b ON a.recipient_duns = b.vendor_duns
WHERE a.recipient_duns IS NOT NULL AND a.recipient_duns != ''
  AND b.vendor_duns IS NOT NULL AND b.vendor_duns != ''
ORDER BY a.recipient_duns
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// directUEISQL generates SQL for USAspending → FPDS direct UEI matching.
func directUEISQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.recipient_uei)
    'usaspending_awards',
    a.award_id,
    'fpds_contracts',
    b.contract_id,
    a.recipient_name,
    'direct_uei',
    1.00
FROM fed_data.usaspending_awards a
JOIN fed_data.fpds_contracts b ON a.recipient_uei = b.vendor_uei
WHERE a.recipient_uei IS NOT NULL AND a.recipient_uei != ''
  AND b.vendor_uei IS NOT NULL AND b.vendor_uei != ''
ORDER BY a.recipient_uei
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// cikNCENEdgarSQL generates SQL for N-CEN registrant → EDGAR direct CIK matching.
// Uses DISTINCT ON to pick the latest filing per CIK.
func cikNCENEdgarSQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (r.cik)
    'ncen_registrants',
    r.accession_number,
    'edgar_entities',
    e.cik,
    r.registrant_name,
    'direct_cik',
    1.00
FROM fed_data.ncen_registrants r
JOIN fed_data.edgar_entities e ON r.cik = e.cik
WHERE r.cik IS NOT NULL
  AND r.cik != ''
ORDER BY r.cik, r.filing_date DESC NULLS LAST
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// crdNCENAdvSQL generates SQL for N-CEN adviser → ADV firm direct CRD matching.
// Filters to numeric CRD values only and deduplicates by adviser_crd.
func crdNCENAdvSQL() string {
	return `
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (a.adviser_crd)
    'ncen_advisers',
    a.adviser_crd,
    'adv_firms',
    b.crd_number::TEXT,
    a.adviser_name,
    'direct_crd',
    1.00
FROM fed_data.ncen_advisers a
JOIN fed_data.adv_firms b ON a.adviser_crd::INTEGER = b.crd_number
WHERE a.adviser_crd IS NOT NULL
  AND a.adviser_crd ~ '^\d+$'
ORDER BY a.adviser_crd
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`
}

// nameStateNCENAdvSQL generates SQL for N-CEN registrant → ADV firm exact
// name+state matching. Normalizes N-CEN state from "US-XX" to bare two-letter code.
func nameStateNCENAdvSQL(normFn func(string) string) string {
	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (r.accession_number, b.crd_number::TEXT)
    'ncen_registrants',
    r.accession_number,
    'adv_firms',
    b.crd_number::TEXT,
    r.registrant_name,
    'exact_name_state',
    0.88
FROM fed_data.ncen_registrants r
JOIN fed_data.adv_firms b
    ON %s = %s
    AND REPLACE(r.state, 'US-', '') = b.state
WHERE r.registrant_name IS NOT NULL AND r.registrant_name != ''
  AND b.firm_name IS NOT NULL AND b.firm_name != ''
  AND r.state IS NOT NULL AND r.state != ''
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref_multi x
      WHERE x.source_dataset = 'ncen_registrants' AND x.source_id = r.accession_number
        AND x.target_dataset = 'adv_firms' AND x.target_id = b.crd_number::TEXT
  )
ORDER BY r.accession_number, b.crd_number::TEXT
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		normFn("r.registrant_name"),
		normFn("b.firm_name"),
	)
}

// nameStateNCENFpdsSQL generates SQL for N-CEN registrant → FPDS contract exact
// name+state matching. Normalizes N-CEN state from "US-XX" to bare two-letter code.
func nameStateNCENFpdsSQL(normFn func(string) string) string {
	return fmt.Sprintf(`
INSERT INTO fed_data.entity_xref_multi
    (source_dataset, source_id, target_dataset, target_id, entity_name, match_type, confidence)
SELECT DISTINCT ON (r.accession_number, f.contract_id::TEXT)
    'ncen_registrants',
    r.accession_number,
    'fpds_contracts',
    f.contract_id::TEXT,
    r.registrant_name,
    'exact_name_state',
    0.88
FROM fed_data.ncen_registrants r
JOIN fed_data.fpds_contracts f
    ON %s = %s
    AND REPLACE(r.state, 'US-', '') = f.vendor_state
WHERE r.registrant_name IS NOT NULL AND r.registrant_name != ''
  AND f.vendor_name IS NOT NULL AND f.vendor_name != ''
  AND r.state IS NOT NULL AND r.state != ''
  AND NOT EXISTS (
      SELECT 1 FROM fed_data.entity_xref_multi x
      WHERE x.source_dataset = 'ncen_registrants' AND x.source_id = r.accession_number
        AND x.target_dataset = 'fpds_contracts' AND x.target_id = f.contract_id::TEXT
  )
ORDER BY r.accession_number, f.contract_id::TEXT
ON CONFLICT (source_dataset, source_id, target_dataset, target_id) DO NOTHING`,
		normFn("r.registrant_name"),
		normFn("f.vendor_name"),
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
