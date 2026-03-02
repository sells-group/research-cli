package company

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errMatchStore wraps mockStore but returns an error on UpsertMatch.
type errMatchStore struct {
	mockStore
	upsertErr error
}

func (m *errMatchStore) UpsertMatch(_ context.Context, _ *Match) error {
	return m.upsertErr
}

func TestLinker_MatchEIN(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"sponsor_dfe_name"}).
		AddRow("ACME Corp 401(k) Plan")
	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("123456789").
		WillReturnRows(rows)

	n, err := linker.matchEIN(context.Background(), 42, "123456789")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify the match was upserted with correct fields.
	require.Len(t, store.matches[42], 1)
	m := store.matches[42][0]
	assert.Equal(t, int64(42), m.CompanyID)
	assert.Equal(t, "form_5500", m.MatchedSource)
	assert.Equal(t, "123456789", m.MatchedKey)
	assert.Equal(t, "direct_ein", m.MatchType)
	require.NotNil(t, m.Confidence)
	assert.Equal(t, 1.0, *m.Confidence)

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("999999999").
		WillReturnError(errors.New("connection refused"))

	n, err := linker.matchEIN(context.Background(), 42, "999999999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "link: query form_5500")
	assert.Equal(t, 0, n)

	assert.Empty(t, store.matches[42])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_NoRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("000000000").
		WillReturnError(pgx.ErrNoRows)

	n, err := linker.matchEIN(context.Background(), 42, "000000000")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	assert.Empty(t, store.matches[42])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &errMatchStore{
		mockStore: *newMockStore(),
		upsertErr: errors.New("upsert failed"),
	}
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"sponsor_dfe_name"}).
		AddRow("Test Plan")
	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("111222333").
		WillReturnRows(rows)

	n, err := linker.matchEIN(context.Background(), 42, "111222333")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert failed")
	assert.Equal(t, 0, n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEOBMF(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"name"}).
		AddRow("COMMUNITY FOUNDATION INC")
	pool.ExpectQuery("SELECT name FROM fed_data.eo_bmf").
		WithArgs("123456789").
		WillReturnRows(rows)

	n, err := linker.matchEOBMF(context.Background(), 42, "123456789")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	require.Len(t, store.matches[42], 1)
	m := store.matches[42][0]
	assert.Equal(t, int64(42), m.CompanyID)
	assert.Equal(t, "eo_bmf", m.MatchedSource)
	assert.Equal(t, "123456789", m.MatchedKey)
	assert.Equal(t, "direct_ein", m.MatchType)
	require.NotNil(t, m.Confidence)
	assert.Equal(t, 1.0, *m.Confidence)

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEOBMF_NoRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT name FROM fed_data.eo_bmf").
		WithArgs("000000000").
		WillReturnError(pgx.ErrNoRows)

	n, err := linker.matchEOBMF(context.Background(), 42, "000000000")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	assert.Empty(t, store.matches[42])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEOBMF_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT name FROM fed_data.eo_bmf").
		WithArgs("999999999").
		WillReturnError(errors.New("connection refused"))

	n, err := linker.matchEOBMF(context.Background(), 42, "999999999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "link: query eo_bmf")
	assert.Equal(t, 0, n)

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEOBMF_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &errMatchStore{
		mockStore: *newMockStore(),
		upsertErr: errors.New("upsert failed"),
	}
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"name"}).
		AddRow("TEST FOUNDATION")
	pool.ExpectQuery("SELECT name FROM fed_data.eo_bmf").
		WithArgs("111222333").
		WillReturnRows(rows)

	n, err := linker.matchEOBMF(context.Background(), 42, "111222333")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert failed")
	assert.Equal(t, 0, n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestPtrFloat(t *testing.T) {
	p := ptrFloat(0.95)
	require.NotNil(t, p)
	assert.Equal(t, 0.95, *p)
}
