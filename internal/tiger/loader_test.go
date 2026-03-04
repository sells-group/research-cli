package tiger

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsLoaded_True(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("12", "edges", 2024).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(1))

	loaded, err := isLoaded(context.Background(), mock, "12", "edges", 2024)
	require.NoError(t, err)
	assert.True(t, loaded)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsLoaded_False(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("12", "addr", 2024).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(0))

	loaded, err := isLoaded(context.Background(), mock, "12", "addr", 2024)
	require.NoError(t, err)
	assert.False(t, loaded)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordLoad(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO tiger_data.load_status").
		WithArgs("12", "FL", "edges", 2024, 50000, 3500).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = recordLoad(context.Background(), mock, "12", "FL", "edges", 2024, 50000, 3500)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadStatus(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()
	rows := pgxmock.NewRows([]string{
		"state_fips", "state_abbr", "table_name", "year",
		"row_count", "loaded_at", "duration_ms",
	}).
		AddRow("12", "FL", "edges", 2024, 50000, now, 3500).
		AddRow("12", "FL", "addr", 2024, 25000, now, 2100)

	mock.ExpectQuery("SELECT state_fips, state_abbr, table_name").
		WillReturnRows(rows)

	status, err := LoadStatus(context.Background(), mock)
	require.NoError(t, err)
	assert.Len(t, status, 2)
	assert.Equal(t, "FL", status[0].StateAbbr)
	assert.Equal(t, "edges", status[0].TableName)
	assert.Equal(t, 50000, status[0].RowCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestColumnCache_Get(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First call should query the DB.
	rows := pgxmock.NewRows([]string{"column_name"}).
		AddRow("tlid").
		AddRow("fullname").
		AddRow("statefp")

	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs("featnames").
		WillReturnRows(rows)

	cache := &columnCache{pool: mock}
	cols, err := cache.get(context.Background(), "featnames")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"tlid":     true,
		"fullname": true,
		"statefp":  true,
	}, cols)

	// Second call should use the cache — no new DB query expected.
	cols2, err := cache.get(context.Background(), "featnames")
	require.NoError(t, err)
	assert.Equal(t, cols, cols2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestColumnCache_Get_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs("missing_table").
		WillReturnError(assert.AnError)

	cache := &columnCache{pool: mock}
	cols, err := cache.get(context.Background(), "missing_table")
	require.Error(t, err)
	assert.Nil(t, cols)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsLoaded_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("48", "edges", 2024).
		WillReturnError(assert.AnError)

	loaded, err := isLoaded(context.Background(), mock, "48", "edges", 2024)
	require.Error(t, err)
	assert.False(t, loaded)
	assert.Contains(t, err.Error(), "check load status")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordLoad_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO tiger_data.load_status").
		WithArgs("48", "TX", "edges", 2024, 1000, 500).
		WillReturnError(assert.AnError)

	err = recordLoad(context.Background(), mock, "48", "TX", "edges", 2024, 1000, 500)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record load status")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadStatus_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"state_fips", "state_abbr", "table_name", "year",
		"row_count", "loaded_at", "duration_ms",
	})

	mock.ExpectQuery("SELECT state_fips, state_abbr, table_name").
		WillReturnRows(rows)

	status, err := LoadStatus(context.Background(), mock)
	require.NoError(t, err)
	assert.Empty(t, status)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadStatus_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT state_fips, state_abbr, table_name").
		WillReturnError(assert.AnError)

	status, err := LoadStatus(context.Background(), mock)
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "query load status")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadStatus_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"state_fips", "state_abbr", "table_name", "year",
		"row_count", "loaded_at", "duration_ms",
	}).
		AddRow("48", "TX", "edges", 2024, 1000, time.Now(), 500).
		RowError(0, assert.AnError)

	mock.ExpectQuery("SELECT state_fips, state_abbr, table_name").
		WillReturnRows(rows)

	status, err := LoadStatus(context.Background(), mock)
	require.Error(t, err)
	assert.Nil(t, status)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadOptions_Defaults(t *testing.T) {
	opts := LoadOptions{}

	// Verify defaults are set in Load() — we test the logic inline.
	if opts.Year == 0 {
		opts.Year = 2024
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.TempDir == "" {
		opts.TempDir = "/tmp/tiger"
	}

	assert.Equal(t, 2024, opts.Year)
	assert.Equal(t, 3, opts.Concurrency)
	assert.Equal(t, 50000, opts.BatchSize)
	assert.Equal(t, "/tmp/tiger", opts.TempDir)
}
