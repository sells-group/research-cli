package resolve

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.NewNop())
}

// --- SQL content tests ---

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

// --- XrefBuilder pgxmock tests ---

func TestNewXrefBuilder(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	xb := NewXrefBuilder(mock)
	assert.NotNil(t, xb)
}

func TestXrefBuilder_Build_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	// Pass 1: direct CRD-CIK
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 50))
	// Pass 2: SIC code
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 30))
	// Pass 3: fuzzy name
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 20))

	xb := NewXrefBuilder(mock)
	total, err := xb.Build(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), total)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefBuilder_Build_TruncateError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnError(fmt.Errorf("permission denied"))

	xb := NewXrefBuilder(mock)
	_, err = xb.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncate entity_xref")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefBuilder_Build_Pass1Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(fmt.Errorf("adv_firms does not exist"))

	xb := NewXrefBuilder(mock)
	_, err = xb.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pass 1")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefBuilder_Build_Pass2Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	// Pass 1 succeeds
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 10))
	// Pass 2 fails
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(fmt.Errorf("sic column missing"))

	xb := NewXrefBuilder(mock)
	_, err = xb.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pass 2")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefBuilder_Build_Pass3Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	// Pass 1 succeeds
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 10))
	// Pass 2 succeeds
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 5))
	// Pass 3 fails
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(fmt.Errorf("pg_trgm not installed"))

	xb := NewXrefBuilder(mock)
	_, err = xb.Build(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pass 3")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefBuilder_Build_ZeroMatches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	mock.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	xb := NewXrefBuilder(mock)
	total, err := xb.Build(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.NoError(t, mock.ExpectationsWereMet())
}
