package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
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

func TestPostgresStore_SaveProvenance(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO field_provenance`).
		WithArgs(
			"run-1", "https://acme.com", "revenue", "website", "5000000",
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), true,
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), false,
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	records := []model.FieldProvenance{
		{
			RunID:               "run-1",
			CompanyURL:          "https://acme.com",
			FieldKey:            "revenue",
			WinnerSource:        "website",
			WinnerValue:         "5000000",
			RawConfidence:       0.85,
			EffectiveConfidence: 0.90,
			Threshold:           0.70,
			ThresholdMet:        true,
		},
	}

	err := s.SaveProvenance(context.Background(), records)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_SaveProvenance_Empty(t *testing.T) {
	s, _ := newMockPostgresStore(t)

	err := s.SaveProvenance(context.Background(), nil)
	require.NoError(t, err)
}

func TestPostgresStore_GetProvenance(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	rows := pgxmock.NewRows([]string{
		"id", "run_id", "company_url", "field_key", "winner_source", "winner_value",
		"raw_confidence", "effective_confidence", "data_as_of", "threshold", "threshold_met",
		"attempts", "premium_cost_usd", "previous_value", "previous_run_id", "value_changed", "created_at",
	}).AddRow(
		int64(1), "run-1", "https://acme.com", "revenue", "website", "5000000",
		0.85, 0.90, (*time.Time)(nil), 0.70, true,
		[]byte(`[{"source":"website","value":"5000000","confidence":0.85,"tier":1}]`),
		0.05, (*string)(nil), (*string)(nil), false, time.Now(),
	)

	mock.ExpectQuery(`SELECT .+ FROM field_provenance WHERE run_id`).
		WithArgs("run-1").
		WillReturnRows(rows)

	got, err := s.GetProvenance(context.Background(), "run-1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "revenue", got[0].FieldKey)
	assert.Equal(t, "website", got[0].WinnerSource)
	assert.InDelta(t, 0.85, got[0].RawConfidence, 0.001)
	assert.True(t, got[0].ThresholdMet)
	require.Len(t, got[0].Attempts, 1)
	assert.Equal(t, "website", got[0].Attempts[0].Source)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresStore_GetLatestProvenance_NotFound(t *testing.T) {
	s, mock := newMockPostgresStore(t)

	mock.ExpectQuery(`SELECT .+ FROM field_provenance`).
		WithArgs("https://unknown.com").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "run_id", "company_url", "field_key", "winner_source", "winner_value",
			"raw_confidence", "effective_confidence", "data_as_of", "threshold", "threshold_met",
			"attempts", "premium_cost_usd", "previous_value", "previous_run_id", "value_changed", "created_at",
		}))

	got, err := s.GetLatestProvenance(context.Background(), "https://unknown.com")
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.NoError(t, mock.ExpectationsWereMet())
}
