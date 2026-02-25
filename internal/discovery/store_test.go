package discovery

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresStore_CreateRun(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectQuery(`INSERT INTO discovery_runs`).
		WithArgs("ppp", pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("550e8400-e29b-41d4-a716-446655440000"))

	id, err := store.CreateRun(context.Background(), RunConfig{
		Strategy: "ppp",
		Params:   map[string]any{"test": true},
	})

	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_CompleteRun(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectExec(`UPDATE discovery_runs SET`).
		WithArgs("run-123", 50, 30, 1.60).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = store.CompleteRun(context.Background(), "run-123", &RunResult{
		CandidatesFound:     50,
		CandidatesQualified: 30,
		CostUSD:             1.60,
	})

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_FailRun(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectExec(`UPDATE discovery_runs SET status = 'failed'`).
		WithArgs("run-456", "something broke").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = store.FailRun(context.Background(), "run-456", "something broke")

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_UpdateCandidateScore(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectExec(`UPDATE discovery_candidates SET score_t1 = \$2 WHERE id = \$1`).
		WithArgs(int64(42), 0.85).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = store.UpdateCandidateScore(context.Background(), 42, "t1", 0.85)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_UpdateCandidateScore_InvalidTier(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	err = store.UpdateCandidateScore(context.Background(), 1, "t99", 0.5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown tier")
}

func TestPostgresStore_DisqualifyCandidate(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectExec(`UPDATE discovery_candidates SET disqualified = true`).
		WithArgs(int64(99), "no_website").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = store.DisqualifyCandidate(context.Background(), 99, "no_website")

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_PlaceIDExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("ChIJ-test123").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := store.PlaceIDExists(context.Background(), "ChIJ-test123")

	require.NoError(t, err)
	assert.True(t, exists)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_DomainExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("example.com").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	exists, err := store.DomainExists(context.Background(), "example.com")

	require.NoError(t, err)
	assert.False(t, exists)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTierColumn(t *testing.T) {
	tests := []struct {
		tier string
		col  string
		err  bool
	}{
		{"t0", "score_t0", false},
		{"t1", "score_t1", false},
		{"t2", "score_t2", false},
		{"t99", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.tier, func(t *testing.T) {
			col, err := tierColumn(tt.tier)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.col, col)
			}
		})
	}
}
