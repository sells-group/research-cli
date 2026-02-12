package ppp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockQuerier implements the Querier interface for testing.
type mockQuerier struct {
	findLoansFunc func(ctx context.Context, name, state, city string) ([]LoanMatch, error)
	closed        bool
}

func (m *mockQuerier) FindLoans(ctx context.Context, name, state, city string) ([]LoanMatch, error) {
	return m.findLoansFunc(ctx, name, state, city)
}

func (m *mockQuerier) Close() { m.closed = true }

// Ensure mockQuerier implements Querier.
var _ Querier = (*mockQuerier)(nil)

func TestQuerier_Tier1Hit(t *testing.T) {
	expected := []LoanMatch{
		{
			LoanNumber:      12345,
			BorrowerName:    "ACME CORP",
			BorrowerState:   "TX",
			CurrentApproval: 150000,
			MatchTier:       1,
			MatchScore:      1.0,
		},
	}

	q := &mockQuerier{
		findLoansFunc: func(_ context.Context, name, state, city string) ([]LoanMatch, error) {
			assert.Equal(t, "Acme Corp", name)
			assert.Equal(t, "TX", state)
			assert.Equal(t, "Austin", city)
			return expected, nil
		},
	}

	matches, err := q.FindLoans(context.Background(), "Acme Corp", "TX", "Austin")
	assert.NoError(t, err)
	assert.Equal(t, expected, matches)
	assert.Equal(t, 1, matches[0].MatchTier)
	assert.Equal(t, 1.0, matches[0].MatchScore)
}

func TestQuerier_Tier2Hit(t *testing.T) {
	expected := []LoanMatch{
		{
			LoanNumber:      67890,
			BorrowerName:    "ACME CORP LLC",
			BorrowerState:   "TX",
			CurrentApproval: 75000,
			MatchTier:       2,
			MatchScore:      0.8,
		},
	}

	q := &mockQuerier{
		findLoansFunc: func(_ context.Context, _, _, _ string) ([]LoanMatch, error) {
			return expected, nil
		},
	}

	matches, err := q.FindLoans(context.Background(), "Acme Corp LLC", "TX", "Austin")
	assert.NoError(t, err)
	assert.Len(t, matches, 1)
	assert.Equal(t, 2, matches[0].MatchTier)
	assert.Equal(t, 0.8, matches[0].MatchScore)
}

func TestQuerier_AllTiersMiss(t *testing.T) {
	q := &mockQuerier{
		findLoansFunc: func(_ context.Context, _, _, _ string) ([]LoanMatch, error) {
			return nil, nil
		},
	}

	matches, err := q.FindLoans(context.Background(), "Nonexistent Corp", "TX", "Austin")
	assert.NoError(t, err)
	assert.Empty(t, matches)
}

func TestQuerier_ErrorPropagation(t *testing.T) {
	expectedErr := errors.New("database connection lost")

	q := &mockQuerier{
		findLoansFunc: func(_ context.Context, _, _, _ string) ([]LoanMatch, error) {
			return nil, expectedErr
		},
	}

	matches, err := q.FindLoans(context.Background(), "Acme Corp", "TX", "Austin")
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, matches)
}

func TestQuerier_Close(t *testing.T) {
	q := &mockQuerier{
		findLoansFunc: func(_ context.Context, _, _, _ string) ([]LoanMatch, error) {
			return nil, nil
		},
	}

	q.Close()
	assert.True(t, q.closed)
}

func TestLoanMatch_Fields(t *testing.T) {
	m := LoanMatch{
		LoanNumber:        99999,
		BorrowerName:      "TEST COMPANY",
		BorrowerAddress:   "123 Main St",
		BorrowerCity:      "Austin",
		BorrowerState:     "TX",
		BorrowerZip:       "78701",
		CurrentApproval:   250000.50,
		ForgivenessAmount: 250000.50,
		JobsReported:      15,
		DateApproved:      time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC),
		LoanStatus:        "Paid in Full",
		BusinessType:      "Corporation",
		NAICSCode:         "541511",
		BusinessAge:       "Existing or more than 2 years old",
		MatchTier:         1,
		MatchScore:        1.0,
	}

	assert.Equal(t, int64(99999), m.LoanNumber)
	assert.Equal(t, "TEST COMPANY", m.BorrowerName)
	assert.Equal(t, 250000.50, m.CurrentApproval)
	assert.Equal(t, 15, m.JobsReported)
	assert.Equal(t, 1, m.MatchTier)
	assert.Equal(t, 1.0, m.MatchScore)
}
