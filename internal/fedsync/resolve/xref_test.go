package resolve

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPass1DirectSQL(t *testing.T) {
	sql := Pass1DirectSQL()
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref")
	assert.Contains(t, sql, "direct_sec_number")
	assert.Contains(t, sql, "adv_firms")
	assert.Contains(t, sql, "edgar_entities")
	assert.Contains(t, sql, "LPAD")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestPass2SICSQL(t *testing.T) {
	sql := Pass2SICSQL()
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref")
	assert.Contains(t, sql, "sic_exact_name")
	assert.Contains(t, sql, "'6211'")
	assert.Contains(t, sql, "'6282'")
	assert.Contains(t, sql, "UPPER(TRIM(a.firm_name))")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestFuzzyMatchSQL(t *testing.T) {
	sql := FuzzyMatchSQL()
	assert.Contains(t, sql, "INSERT INTO fed_data.entity_xref")
	assert.Contains(t, sql, "fuzzy_name")
	assert.Contains(t, sql, "similarity")
	assert.Contains(t, sql, "0.6")
	assert.Contains(t, sql, "DISTINCT ON")
	assert.Contains(t, sql, "ON CONFLICT")
}

func TestPass1DirectSQL_NotEmpty(t *testing.T) {
	sql := Pass1DirectSQL()
	assert.NotEmpty(t, strings.TrimSpace(sql))
}

func TestPass2SICSQL_NotEmpty(t *testing.T) {
	sql := Pass2SICSQL()
	assert.NotEmpty(t, strings.TrimSpace(sql))
}

func TestFuzzyMatchSQL_NotEmpty(t *testing.T) {
	sql := FuzzyMatchSQL()
	assert.NotEmpty(t, strings.TrimSpace(sql))
}

func TestAllSQLHaveConflictClause(t *testing.T) {
	queries := []struct {
		name string
		sql  string
	}{
		{"pass1", Pass1DirectSQL()},
		{"pass2", Pass2SICSQL()},
		{"fuzzy", FuzzyMatchSQL()},
	}
	for _, q := range queries {
		assert.Contains(t, q.sql, "ON CONFLICT", "query %s should have ON CONFLICT clause", q.name)
		assert.Contains(t, q.sql, "DO NOTHING", "query %s should have DO NOTHING", q.name)
	}
}

func TestPass1DirectSQL_MatchType(t *testing.T) {
	sql := Pass1DirectSQL()
	assert.Contains(t, sql, "'direct_sec_number'")
	assert.Contains(t, sql, "1.00")
}

func TestPass2SICSQL_MatchType(t *testing.T) {
	sql := Pass2SICSQL()
	assert.Contains(t, sql, "'sic_exact_name'")
	assert.Contains(t, sql, "0.95")
}

func TestFuzzyMatchSQL_MatchType(t *testing.T) {
	sql := FuzzyMatchSQL()
	assert.Contains(t, sql, "'fuzzy_name'")
}
