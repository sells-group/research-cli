package resolve

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeName_Empty(t *testing.T) {
	assert.Equal(t, "", NormalizeName(""))
	assert.Equal(t, "", NormalizeName("   "))
}

func TestNormalizeName_Uppercase(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors"))
}

func TestNormalizeName_StripLLC(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors LLC"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors L.L.C."))
}

func TestNormalizeName_StripInc(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Inc"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Inc."))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Incorporated"))
}

func TestNormalizeName_StripCorp(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Corp"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Corp."))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Corporation"))
}

func TestNormalizeName_StripLtd(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Ltd"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Ltd."))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors Limited"))
}

func TestNormalizeName_StripLP(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors LP"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors L.P."))
}

func TestNormalizeName_StripLLP(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors LLP"))
}

func TestNormalizeName_StripDBA(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors DBA"))
	assert.Equal(t, "ACME ADVISORS", NormalizeName("Acme Advisors D/B/A"))
}

func TestNormalizeName_Punctuation(t *testing.T) {
	assert.Equal(t, "SMITH AND JONES", NormalizeName("Smith & Jones"))
	assert.Equal(t, "SMITH AND JONES", NormalizeName("Smith & Jones,"))
	assert.Equal(t, "JOES ADVISORS", NormalizeName("Joe's Advisors"))
}

func TestNormalizeName_DashToSpace(t *testing.T) {
	assert.Equal(t, "WELLS FARGO", NormalizeName("Wells-Fargo"))
}

func TestNormalizeName_CollapseSpaces(t *testing.T) {
	assert.Equal(t, "ACME ADVISORS", NormalizeName("  Acme   Advisors  "))
}

func TestNormalizeName_CombinedNormalization(t *testing.T) {
	// Real-world example: complex name with multiple artifacts.
	assert.Equal(t, "RAYMOND JAMES AND ASSOCIATES", NormalizeName("Raymond James & Associates, Inc."))
}

func TestNormalizeName_OnlySuffix(t *testing.T) {
	// Edge case: name is just a legal suffix — not stripped since suffixes
	// require a space prefix (e.g., " LLC" not "LLC" alone).
	assert.Equal(t, "LLC", NormalizeName("LLC"))
}

func TestNormalizeName_PreservesContent(t *testing.T) {
	// Names without suffixes or punctuation should be preserved.
	assert.Equal(t, "VANGUARD GROUP", NormalizeName("Vanguard Group"))
}

func TestNormalizeNameSQL_NotEmpty(t *testing.T) {
	sql := NormalizeNameSQL("a.firm_name")
	assert.NotEmpty(t, sql)
	assert.Contains(t, sql, "a.firm_name")
	assert.Contains(t, sql, "UPPER")
	assert.Contains(t, sql, "TRIM")
	assert.Contains(t, sql, "REGEXP_REPLACE")
}

func TestNormalizeNameSQL_ContainsLegalSuffixPattern(t *testing.T) {
	sql := NormalizeNameSQL("col")
	assert.Contains(t, sql, "LLC")
	assert.Contains(t, sql, "INC")
	assert.Contains(t, sql, "CORP")
}
