package resolve

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- SQL content tests ---

func TestDirectCRDSQL_Content(t *testing.T) {
	sql := directCRDSQL("adv_firms", "brokercheck", "a.firm_name")
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref_multi")
	assert.Contains(t, sql, "'adv_firms'")
	assert.Contains(t, sql, "'brokercheck'")
	assert.Contains(t, sql, "'direct_crd'")
	assert.Contains(t, sql, "1.00")
	assert.Contains(t, sql, "a.crd_number = b.crd_number")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestDirectCRDSQL_FormBD(t *testing.T) {
	sql := directCRDSQL("adv_firms", "form_bd", "a.firm_name")
	assert.Contains(t, sql, "'adv_firms'")
	assert.Contains(t, sql, "'form_bd'")
	assert.Contains(t, sql, "'direct_crd'")
}

func TestCIKAdvEdgarSQL_Content(t *testing.T) {
	sql := cikAdvEdgarSQL()
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref_multi")
	assert.Contains(t, sql, "'adv_firms'")
	assert.Contains(t, sql, "'edgar_entities'")
	assert.Contains(t, sql, "'direct_cik'")
	assert.Contains(t, sql, "LPAD")
	assert.Contains(t, sql, "sec_number")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestCIKFormDEdgarSQL_Content(t *testing.T) {
	sql := cikFormDEdgarSQL()
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref_multi")
	assert.Contains(t, sql, "'form_d'")
	assert.Contains(t, sql, "'edgar_entities'")
	assert.Contains(t, sql, "'direct_cik'")
	assert.Contains(t, sql, "f.cik = e.cik")
	assert.Contains(t, sql, "DISTINCT ON")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestExactNameGeoSQL_Zip(t *testing.T) {
	normName := NormalizeNameSQL
	sql := exactNameGeoSQL(
		"fpds_contracts", "contract_id", "vendor_name", "vendor_zip",
		"ppp_loans", "loannumber", "borrowername", "borrowerzip",
		"zip", 0.92, normName,
	)
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref_multi")
	assert.Contains(t, sql, "'fpds_contracts'")
	assert.Contains(t, sql, "'ppp_loans'")
	assert.Contains(t, sql, "'exact_name_zip'")
	assert.Contains(t, sql, "0.92")
	assert.Contains(t, sql, "LEFT(a.vendor_zip, 5)")
	assert.Contains(t, sql, "LEFT(b.borrowerzip, 5)")
	assert.Contains(t, sql, "UPPER")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestExactNameGeoSQL_State(t *testing.T) {
	normName := NormalizeNameSQL
	sql := exactNameGeoSQL(
		"adv_firms", "crd_number", "firm_name", "state",
		"osha_inspections", "activity_nr", "estab_name", "site_state",
		"state", 0.88, normName,
	)
	assert.Contains(t, sql, "'adv_firms'")
	assert.Contains(t, sql, "'osha_inspections'")
	assert.Contains(t, sql, "'exact_name_state'")
	assert.Contains(t, sql, "0.88")
	assert.Contains(t, sql, "a.state = b.site_state")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestExactNameGeoSQL_NotExists(t *testing.T) {
	normName := NormalizeNameSQL
	sql := exactNameGeoSQL(
		"adv_firms", "crd_number", "firm_name", "state",
		"epa_facilities", "registry_id", "fac_name", "fac_state",
		"state", 0.88, normName,
	)
	assert.Contains(t, sql, "NOT EXISTS")
	assert.Contains(t, sql, "entity_xref_multi")
}

func TestFuzzyNameStateSQL_Content(t *testing.T) {
	sql := fuzzyNameStateSQL(
		"adv_firms", "crd_number", "firm_name", "state",
		"ppp_loans", "loannumber", "borrowername", "borrowerstate",
	)
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref_multi")
	assert.Contains(t, sql, "'adv_firms'")
	assert.Contains(t, sql, "'ppp_loans'")
	assert.Contains(t, sql, "'fuzzy_name_state'")
	assert.Contains(t, sql, "similarity")
	assert.Contains(t, sql, "0.6")
	assert.Contains(t, sql, "DISTINCT ON")
	assert.Contains(t, sql, "a.state = b.borrowerstate")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestFuzzyNameStateSQL_EdgarFPDS(t *testing.T) {
	sql := fuzzyNameStateSQL(
		"edgar_entities", "cik", "entity_name", "state_of_business",
		"fpds_contracts", "contract_id", "vendor_name", "vendor_state",
	)
	assert.Contains(t, sql, "'edgar_entities'")
	assert.Contains(t, sql, "'fpds_contracts'")
	assert.Contains(t, sql, "a.state_of_business = b.vendor_state")
}

func TestAllPasses_Count(t *testing.T) {
	passes := allPasses()
	assert.Len(t, passes, 15)
}

func TestAllPasses_UniqueNames(t *testing.T) {
	passes := allPasses()
	seen := make(map[string]bool)
	for _, p := range passes {
		assert.False(t, seen[p.name], "duplicate pass name: %s", p.name)
		seen[p.name] = true
	}
}

func TestAllPasses_AllHaveConflictClause(t *testing.T) {
	for _, p := range allPasses() {
		assert.Contains(t, p.sql, "ON CONFLICT", "pass %s missing ON CONFLICT", p.name)
		assert.Contains(t, p.sql, "DO NOTHING", "pass %s missing DO NOTHING", p.name)
	}
}

func TestAllPasses_AllInsertIntoMulti(t *testing.T) {
	for _, p := range allPasses() {
		assert.Contains(t, p.sql, "INSERT INTO fed_data.entity_xref_multi",
			"pass %s missing INSERT INTO entity_xref_multi", p.name)
	}
}

func TestAllPasses_MatchTypes(t *testing.T) {
	passes := allPasses()
	matchTypes := make(map[string]bool)

	for _, p := range passes {
		for _, mt := range []string{"direct_crd", "direct_cik", "exact_name_zip", "exact_name_state", "fuzzy_name_state"} {
			quoted := "'" + mt + "'"
			if strings.Contains(p.sql, quoted) {
				matchTypes[mt] = true
			}
		}
	}
	assert.True(t, matchTypes["direct_crd"], "missing direct_crd match type")
	assert.True(t, matchTypes["direct_cik"], "missing direct_cik match type")
	assert.True(t, matchTypes["exact_name_zip"], "missing exact_name_zip match type")
	assert.True(t, matchTypes["exact_name_state"], "missing exact_name_state match type")
	assert.True(t, matchTypes["fuzzy_name_state"], "missing fuzzy_name_state match type")
}

// --- MultiXrefBuilder pgxmock tests ---

func TestNewMultiXrefBuilder(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	builder := NewMultiXrefBuilder(mock)
	assert.NotNil(t, builder)
}

func TestMultiXrefBuilder_Build_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Truncate
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))

	// 14 passes, each returns some rows.
	passes := allPasses()
	for range passes {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", 10))
	}

	builder := NewMultiXrefBuilder(mock)
	total, counts, err := builder.Build(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(15*10), total)
	assert.Len(t, counts, 15)
	for _, c := range counts {
		assert.Equal(t, int64(10), c)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiXrefBuilder_Build_TruncateError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnError(fmt.Errorf("permission denied"))

	builder := NewMultiXrefBuilder(mock)
	_, _, err = builder.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncate entity_xref_multi")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiXrefBuilder_Build_PassError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Truncate succeeds
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	// First pass fails
	mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
		WillReturnError(fmt.Errorf("table does not exist"))

	builder := NewMultiXrefBuilder(mock)
	_, _, err = builder.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multi_xref: pass")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiXrefBuilder_Build_MiddlePassError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	// First 3 passes succeed
	for range 3 {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", 5))
	}
	// 4th pass fails
	mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
		WillReturnError(fmt.Errorf("column mismatch"))

	builder := NewMultiXrefBuilder(mock)
	total, counts, err := builder.Build(context.Background())
	require.Error(t, err)
	// First 3 passes should have contributed
	assert.Equal(t, int64(15), total)
	assert.Len(t, counts, 3)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiXrefBuilder_Build_ZeroMatches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	for range allPasses() {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))
	}

	builder := NewMultiXrefBuilder(mock)
	total, counts, err := builder.Build(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), total)
	for _, c := range counts {
		assert.Equal(t, int64(0), c)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiXrefBuilder_Build_VaryingCounts(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))

	passes := allPasses()
	var expectedTotal int64
	for i := range passes {
		n := int64(i * 5)
		expectedTotal += n
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", n))
	}

	builder := NewMultiXrefBuilder(mock)
	total, counts, err := builder.Build(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectedTotal, total)
	assert.Len(t, counts, len(passes))
	assert.NoError(t, mock.ExpectationsWereMet())
}
