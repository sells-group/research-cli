package ppp

import (
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loanColumns are the 14 columns returned by tier 1/2 queries.
var loanColumns = []string{
	"loannumber", "borrowername", "borroweraddress", "borrowercity",
	"borrowerstate", "borrowerzip", "currentapprovalamount", "forgivenessamount",
	"jobsreported", "dateapproved", "loanstatus", "businesstype",
	"naicscode", "businessagedescription",
}

// loanColumnsWithScore are the 15 columns returned by tier 3 queries.
var loanColumnsWithScore = append(append([]string{}, loanColumns...), "sim_score")

func sampleDate() time.Time {
	return time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC)
}

func TestScanLoanMatches_SingleRow(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(12345), "ACME CORP", "123 Main St", "Austin",
			"TX", "78701", 150000.0, 150000.0,
			15, sampleDate(), "Paid in Full", "Corporation",
			"541511", "Existing or more than 2 years old")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatches(pgxRows, 1, 1.0)
	require.NoError(t, err)
	require.Len(t, matches, 1)

	m := matches[0]
	assert.Equal(t, int64(12345), m.LoanNumber)
	assert.Equal(t, "ACME CORP", m.BorrowerName)
	assert.Equal(t, "123 Main St", m.BorrowerAddress)
	assert.Equal(t, "Austin", m.BorrowerCity)
	assert.Equal(t, "TX", m.BorrowerState)
	assert.Equal(t, "78701", m.BorrowerZip)
	assert.Equal(t, 150000.0, m.CurrentApproval)
	assert.Equal(t, 150000.0, m.ForgivenessAmount)
	assert.Equal(t, 15, m.JobsReported)
	assert.Equal(t, sampleDate(), m.DateApproved)
	assert.Equal(t, "Paid in Full", m.LoanStatus)
	assert.Equal(t, "Corporation", m.BusinessType)
	assert.Equal(t, "541511", m.NAICSCode)
	assert.Equal(t, "Existing or more than 2 years old", m.BusinessAge)
	assert.Equal(t, 1, m.MatchTier)
	assert.Equal(t, 1.0, m.MatchScore)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatches_MultipleRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(111), "COMPANY A", "1 A St", "Dallas", "TX", "75001",
			100000.0, 100000.0, 10, sampleDate(), "Active", "LLC", "541511", "New").
		AddRow(int64(222), "COMPANY B", "2 B St", "Houston", "TX", "77001",
			200000.0, 200000.0, 20, sampleDate(), "Active", "Corporation", "541512", "Old")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatches(pgxRows, 2, 0.8)
	require.NoError(t, err)
	require.Len(t, matches, 2)

	assert.Equal(t, int64(111), matches[0].LoanNumber)
	assert.Equal(t, "COMPANY A", matches[0].BorrowerName)
	assert.Equal(t, 2, matches[0].MatchTier)
	assert.Equal(t, 0.8, matches[0].MatchScore)

	assert.Equal(t, int64(222), matches[1].LoanNumber)
	assert.Equal(t, "COMPANY B", matches[1].BorrowerName)
	assert.Equal(t, 2, matches[1].MatchTier)
	assert.Equal(t, 0.8, matches[1].MatchScore)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatches_EmptyResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatches(pgxRows, 1, 1.0)
	require.NoError(t, err)
	assert.Empty(t, matches)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatches_RowError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(111), "COMPANY A", "1 A St", "Dallas", "TX", "75001",
			100000.0, 100000.0, 10, sampleDate(), "Active", "LLC", "541511", "New").
		RowError(0, assert.AnError)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	_, err = scanLoanMatches(pgxRows, 1, 1.0)
	assert.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_SingleRow(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumnsWithScore).
		AddRow(int64(333), "FUZZY MATCH CO", "3 C St", "Austin", "TX", "78702",
			75000.0, 75000.0, 8, sampleDate(), "Active", "LLC", "541511", "New",
			0.85)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatchesWithScore(pgxRows, 3)
	require.NoError(t, err)
	require.Len(t, matches, 1)

	assert.Equal(t, int64(333), matches[0].LoanNumber)
	assert.Equal(t, "FUZZY MATCH CO", matches[0].BorrowerName)
	assert.Equal(t, 3, matches[0].MatchTier)
	assert.InDelta(t, 0.85, matches[0].MatchScore, 0.001)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_MultipleRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumnsWithScore).
		AddRow(int64(444), "HIGH MATCH", "4 D St", "Austin", "TX", "78703",
			50000.0, 50000.0, 5, sampleDate(), "Active", "LLC", "541511", "New",
			0.95).
		AddRow(int64(555), "LOWER MATCH", "5 E St", "Austin", "TX", "78704",
			30000.0, 30000.0, 3, sampleDate(), "Active", "LLC", "541512", "Old",
			0.72)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatchesWithScore(pgxRows, 3)
	require.NoError(t, err)
	require.Len(t, matches, 2)

	assert.InDelta(t, 0.95, matches[0].MatchScore, 0.001)
	assert.InDelta(t, 0.72, matches[1].MatchScore, 0.001)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_EmptyResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumnsWithScore)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	matches, err := scanLoanMatchesWithScore(pgxRows, 3)
	require.NoError(t, err)
	assert.Empty(t, matches)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_RowError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumnsWithScore).
		AddRow(int64(666), "ERR CO", "6 F St", "Austin", "TX", "78705",
			25000.0, 25000.0, 2, sampleDate(), "Active", "LLC", "541511", "New",
			0.60).
		RowError(0, assert.AnError)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	_, err = scanLoanMatchesWithScore(pgxRows, 3)
	assert.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Provide wrong number of columns to trigger a scan error.
	badColumns := []string{"loannumber", "borrowername"}
	rows := pgxmock.NewRows(badColumns).
		AddRow(int64(777), "BAD ROW")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	_, err = scanLoanMatchesWithScore(pgxRows, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scan tier3 row")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatchesWithScore_CloseError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumnsWithScore).CloseError(assert.AnError)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	_, err = scanLoanMatchesWithScore(pgxRows, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rows iteration")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatch_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(777), "SINGLE SCAN CO", "7 G St", "Dallas", "TX", "75002",
			500000.0, 450000.0, 50, sampleDate(), "Paid in Full", "S Corporation",
			"523110", "Existing or more than 2 years old")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	require.True(t, pgxRows.Next())
	m, err := scanLoanMatch(pgxRows)
	require.NoError(t, err)

	assert.Equal(t, int64(777), m.LoanNumber)
	assert.Equal(t, "SINGLE SCAN CO", m.BorrowerName)
	assert.Equal(t, "7 G St", m.BorrowerAddress)
	assert.Equal(t, "Dallas", m.BorrowerCity)
	assert.Equal(t, "TX", m.BorrowerState)
	assert.Equal(t, "75002", m.BorrowerZip)
	assert.Equal(t, 500000.0, m.CurrentApproval)
	assert.Equal(t, 450000.0, m.ForgivenessAmount)
	assert.Equal(t, 50, m.JobsReported)
	assert.Equal(t, "Paid in Full", m.LoanStatus)
	assert.Equal(t, "S Corporation", m.BusinessType)
	assert.Equal(t, "523110", m.NAICSCode)
	assert.Equal(t, "Existing or more than 2 years old", m.BusinessAge)
	// scanLoanMatch does NOT set tier/score — those are set by caller
	assert.Equal(t, 0, m.MatchTier)
	assert.Equal(t, 0.0, m.MatchScore)

	pgxRows.Close()
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanLoanMatches_CloseError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).CloseError(assert.AnError)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pgxRows, err := mock.Query(t.Context(), "SELECT 1")
	require.NoError(t, err)

	_, err = scanLoanMatches(pgxRows, 1, 1.0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rows iteration")

	require.NoError(t, mock.ExpectationsWereMet())
}
