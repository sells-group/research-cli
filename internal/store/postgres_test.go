package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMockPostgresStore creates a PostgresStore backed by pgxmock for unit testing.
func newMockPostgresStore(t *testing.T) (*PostgresStore, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { mock.Close() })

	s := &PostgresStore{pool: mock}
	return s, mock
}

func TestPostgresStore_GetRun_NotFound(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectQuery(`SELECT id, company, status, result, error, created_at, updated_at FROM runs WHERE id = \$1`).
		WithArgs("nonexistent-run").
		WillReturnError(pgx.ErrNoRows)

	_, err := s.GetRun(context.Background(), "nonexistent-run")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_GetCachedCrawl_NotFound(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectQuery(`SELECT id, company_url, pages, crawled_at, expires_at FROM crawl_cache`).
		WithArgs("https://unknown.com").
		WillReturnError(pgx.ErrNoRows)

	result, err := s.GetCachedCrawl(context.Background(), "https://unknown.com")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_GetCachedLinkedIn_NotFound(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectQuery(`SELECT data FROM linkedin_cache`).
		WithArgs("unknown.com").
		WillReturnError(pgx.ErrNoRows)

	result, err := s.GetCachedLinkedIn(context.Background(), "unknown.com")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_GetCachedScrape_NotFound(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectQuery(`SELECT content FROM scrape_cache`).
		WithArgs("abc123hash").
		WillReturnError(pgx.ErrNoRows)

	result, err := s.GetCachedScrape(context.Background(), "abc123hash")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_SetCachedCrawl_Upsert(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectExec(`ON CONFLICT`).
		WithArgs(pgxmock.AnyArg(), "https://acme.com", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err := s.SetCachedCrawl(context.Background(), "https://acme.com", nil, 24*time.Hour)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_SetCachedLinkedIn_Upsert(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectExec(`ON CONFLICT`).
		WithArgs(pgxmock.AnyArg(), "example.com", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	data := []byte(`{"company":"Example Inc"}`)
	err := s.SetCachedLinkedIn(context.Background(), "example.com", data, 48*time.Hour)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_SetCachedScrape_Upsert(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectExec(`ON CONFLICT`).
		WithArgs(pgxmock.AnyArg(), "hash456", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	content := []byte(`{"scraped":"data"}`)
	err := s.SetCachedScrape(context.Background(), "hash456", content, 12*time.Hour)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
