package geoscraper

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geospatial"
)

func TestSanitizeTableParts(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"geo.poi", []string{"geo", "poi"}},
		{"geo.infrastructure", []string{"geo", "infrastructure"}},
		{"simple_table", []string{"simple_table"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeTableParts(tt.input))
		})
	}
}

func TestPostSyncGeocode_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnError(errors.New("query failed"))

	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	err = PostSyncGeocode(context.Background(), mock, queue, "geo.poi", &SyncResult{RowsSynced: 5})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query ungeocoded rows")
}

func TestPostSyncGeocode_NoRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnRows(pgxmock.NewRows([]string{"source_id", "address"}))

	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	err = PostSyncGeocode(context.Background(), mock, queue, "geo.poi", &SyncResult{RowsSynced: 0})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostSyncGeocode_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return rows with a RowError to trigger scan failure.
	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnRows(pgxmock.NewRows([]string{"source_id", "address"}).
			AddRow("src1", "addr1").
			RowError(0, errors.New("row corrupted")))

	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	err = PostSyncGeocode(context.Background(), mock, queue, "geo.poi", &SyncResult{RowsSynced: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postsync: scan row")
}

func TestPostSyncGeocode_EnqueueBatchError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnRows(pgxmock.NewRows([]string{"source_id", "address"}).
			AddRow("src1", "123 Main St"))

	// EnqueueBatch calls pool.Begin which will fail.
	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	err = PostSyncGeocode(context.Background(), mock, queue, "geo.poi", &SyncResult{RowsSynced: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postsync: enqueue batch")
}

func TestPostSyncGeocode_SmallBatchProcessesImmediately(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnRows(pgxmock.NewRows([]string{"source_id", "address"}).
			AddRow("src1", "123 Main St"))

	// EnqueueBatch: Begin + Exec + Commit.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO geo\.geocode_queue`).
		WithArgs("geo.poi", "src1", "123 Main St").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	// ProcessBatch starts Begin, but we return an error to keep the test simple.
	// This covers the "small batch → ProcessBatch attempted" code path.
	mock.ExpectBegin().WillReturnError(errors.New("process batch unavailable"))

	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	err = PostSyncGeocode(context.Background(), mock, queue, "geo.poi", &SyncResult{RowsSynced: 1})
	require.NoError(t, err) // ProcessBatch error is logged, not returned
	require.NoError(t, mock.ExpectationsWereMet())
}
