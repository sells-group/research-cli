package ppp

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestClient creates a Client backed by a pgxmock pool.
func newTestClient(mock pgxmock.PgxPoolIface) *Client {
	return &Client{
		pool: mock,
		cfg:  Config{MaxCandidates: 10, SimilarityThreshold: 0.3},
	}
}

func TestClient_FindLoans_Tier1Hit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(100), "ACME CORP", "1 Main", "Austin", "TX", "78701",
			100000.0, 100000.0, 10, time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC),
			"Active", "LLC", "541511", "New")

	// Tier 1 should match; tiers 2 and 3 should NOT be called.
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME CORP").
		WillReturnRows(rows)

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Acme Corp", "TX", "Austin")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, int64(100), matches[0].LoanNumber)
	assert.Equal(t, 1, matches[0].MatchTier)
	assert.Equal(t, 1.0, matches[0].MatchScore)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier2Fallback(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Tier 1: no results
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME CORP LLC").
		WillReturnRows(pgxmock.NewRows(loanColumns))

	// Tier 2: match found
	tier2Rows := pgxmock.NewRows(loanColumns).
		AddRow(int64(200), "ACME CORP LLC", "2 Second", "Dallas", "TX", "75001",
			50000.0, 50000.0, 5, time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC),
			"Active", "LLC", "541511", "New")
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME CORP").
		WillReturnRows(tier2Rows)

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Acme Corp LLC", "TX", "Dallas")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, 2, matches[0].MatchTier)
	assert.Equal(t, 0.8, matches[0].MatchScore)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier3Fallback(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Tier 1: no results (upperName = "ACME CORP")
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME CORP").
		WillReturnRows(pgxmock.NewRows(loanColumns))

	// Tier 2: no results (normName = Normalize("Acme Corp") = "ACME" since Corp is a suffix)
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME").
		WillReturnRows(pgxmock.NewRows(loanColumns))

	// Tier 3: fuzzy match
	tier3Rows := pgxmock.NewRows(loanColumnsWithScore).
		AddRow(int64(300), "ACME CORPORATION", "3 Third", "Austin", "TX", "78702",
			75000.0, 75000.0, 8, time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC),
			"Active", "Corporation", "541511", "New", 0.85)
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ACME CORP", pgxmock.AnyArg(), 10).
		WillReturnRows(tier3Rows)

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Acme Corp", "TX", "Austin")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, 3, matches[0].MatchTier)
	assert.InDelta(t, 0.85, matches[0].MatchScore, 0.001)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_AllTiersMiss(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// All tiers return empty.
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "NONEXISTENT CO").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "NONEXISTENT").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "NONEXISTENT CO", pgxmock.AnyArg(), 10).
		WillReturnRows(pgxmock.NewRows(loanColumnsWithScore))

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Nonexistent Co", "TX", "")
	require.NoError(t, err)
	assert.Empty(t, matches)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier1Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR CORP").
		WillReturnError(assert.AnError)

	c := newTestClient(mock)
	_, err = c.FindLoans(context.Background(), "Err Corp", "TX", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tier1 query")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier2Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Tier 1: empty
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR CORP").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	// Tier 2: error
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR").
		WillReturnError(assert.AnError)

	c := newTestClient(mock)
	_, err = c.FindLoans(context.Background(), "Err Corp", "TX", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tier2 query")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier3Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Tier 1: empty
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR CORP").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	// Tier 2: empty
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	// Tier 3: error
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("TX", "ERR CORP", pgxmock.AnyArg(), 10).
		WillReturnError(assert.AnError)

	c := newTestClient(mock)
	_, err = c.FindLoans(context.Background(), "Err Corp", "TX", "Austin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tier3 query")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier3WithCity(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Skip to tier 3
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME CO").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME").
		WillReturnRows(pgxmock.NewRows(loanColumns))

	tier3Rows := pgxmock.NewRows(loanColumnsWithScore).
		AddRow(int64(400), "SOME COMPANY", "4 Fourth", "SF", "CA", "94102",
			60000.0, 60000.0, 6, time.Date(2020, 7, 1, 0, 0, 0, 0, time.UTC),
			"Active", "LLC", "541511", "New", 0.78)
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME CO", pgxmock.AnyArg(), 10).
		WillReturnRows(tier3Rows)

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Some Co", "CA", "SF")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.InDelta(t, 0.78, matches[0].MatchScore, 0.001)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_FindLoans_Tier3WithoutCity(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Skip to tier 3 with empty city
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME CO").
		WillReturnRows(pgxmock.NewRows(loanColumns))
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME").
		WillReturnRows(pgxmock.NewRows(loanColumns))

	tier3Rows := pgxmock.NewRows(loanColumnsWithScore)
	mock.ExpectQuery("SELECT loannumber").
		WithArgs("CA", "SOME CO", pgxmock.AnyArg(), 10).
		WillReturnRows(tier3Rows)

	c := newTestClient(mock)
	matches, err := c.FindLoans(context.Background(), "Some Co", "CA", "")
	require.NoError(t, err)
	assert.Empty(t, matches)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClient_Close(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	mock.ExpectClose()

	c := newTestClient(mock)
	c.Close()

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConfig_Fields(t *testing.T) {
	cfg := Config{
		URL:                 "postgres://localhost:5432/ppp",
		SimilarityThreshold: 0.3,
		MaxCandidates:       20,
	}
	assert.Equal(t, "postgres://localhost:5432/ppp", cfg.URL)
	assert.Equal(t, 0.3, cfg.SimilarityThreshold)
	assert.Equal(t, 20, cfg.MaxCandidates)
}
