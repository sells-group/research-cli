package geospatial

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/pkg/geocode"
)

// mockGeocodeClient implements geocode.Client for testing.
type mockGeocodeClient struct {
	result *geocode.Result
	err    error
}

func (m *mockGeocodeClient) Geocode(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
	return m.result, m.err
}

func (m *mockGeocodeClient) BatchGeocode(_ context.Context, _ []geocode.AddressInput) ([]geocode.Result, error) {
	return nil, nil
}

func (m *mockGeocodeClient) ReverseGeocode(_ context.Context, _, _ float64) (*geocode.ReverseResult, error) {
	return nil, nil
}

func TestNewGeocodeQueue_DefaultBatchSize(t *testing.T) {
	q := NewGeocodeQueue(nil, nil, 0)
	assert.Equal(t, 100, q.batchSize)
}

func TestNewGeocodeQueue_CustomBatchSize(t *testing.T) {
	q := NewGeocodeQueue(nil, nil, 50)
	assert.Equal(t, 50, q.batchSize)
}

func TestEnqueue_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("geo.poi", "123", "100 Main St, Miami, FL").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.Enqueue(context.Background(), "geo.poi", "123", "100 Main St, Miami, FL")

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueue_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("geo.poi", "123", "100 Main St").
		WillReturnError(fmt.Errorf("connection refused"))

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.Enqueue(context.Background(), "geo.poi", "123", "100 Main St")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "enqueue")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueBatch_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("geo.poi", "1", "100 Main St").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("geo.poi", "2", "200 Main St").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.EnqueueBatch(context.Background(), "geo.poi", []QueueItem{
		{SourceID: "1", Address: "100 Main St"},
		{SourceID: "2", Address: "200 Main St"},
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueBatch_Empty(t *testing.T) {
	q := NewGeocodeQueue(nil, nil, 100)
	err := q.EnqueueBatch(context.Background(), "geo.poi", nil)
	require.NoError(t, err)
}

func TestEnqueueBatch_ItemError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("geo.poi", "1", "100 Main St").
		WillReturnError(fmt.Errorf("duplicate key"))
	mock.ExpectRollback()

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.EnqueueBatch(context.Background(), "geo.poi", []QueueItem{
		{SourceID: "1", Address: "100 Main St"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "enqueue batch item")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_NoPending(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}))
	mock.ExpectCommit()

	gc := &mockGeocodeClient{}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_GeocodeSuccess(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Claim phase.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(1, "geo.poi", "42", "100 Main St, Miami, FL"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{1}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	// Complete phase.
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(1, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	gc := &mockGeocodeClient{
		result: &geocode.Result{
			Matched:   true,
			Latitude:  25.77,
			Longitude: -80.19,
			Source:    "tiger",
			Quality:   "rooftop",
		},
	}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_GeocodeFailure(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Claim phase.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(5, "geo.poi", "99", "Bad Address"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{5}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	// Failed phase.
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(5, "geocode error").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	gc := &mockGeocodeClient{err: fmt.Errorf("geocode error")}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_MultipleItems(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Claim phase with 2 rows.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(1, "geo.poi", "10", "100 Main St").
				AddRow(2, "geo.poi", "20", "200 Main St"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{1, 2}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))
	mock.ExpectCommit()

	// Both complete.
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(1, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(2, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	gc := &mockGeocodeClient{
		result: &geocode.Result{Matched: true, Latitude: 25.77, Longitude: -80.19, Source: "tiger"},
	}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_BeginError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	q := NewGeocodeQueue(mock, nil, 100)
	_, err = q.ProcessBatch(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_ClaimQueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnError(fmt.Errorf("query error"))
	mock.ExpectRollback()

	q := NewGeocodeQueue(mock, nil, 100)
	_, err = q.ProcessBatch(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteBack_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`UPDATE .+`).
		WithArgs(25.77, -80.19, "42").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.WriteBack(context.Background(), "geo.poi", "42", &geocode.Result{
		Matched:   true,
		Latitude:  25.77,
		Longitude: -80.19,
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteBack_NilResult(t *testing.T) {
	q := NewGeocodeQueue(nil, nil, 100)
	err := q.WriteBack(context.Background(), "geo.poi", "42", nil)
	require.NoError(t, err)
}

func TestWriteBack_UnmatchedResult(t *testing.T) {
	q := NewGeocodeQueue(nil, nil, 100)
	err := q.WriteBack(context.Background(), "geo.poi", "42", &geocode.Result{Matched: false})
	require.NoError(t, err)
}

func TestWriteBack_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`UPDATE .+`).
		WithArgs(25.77, -80.19, "42").
		WillReturnError(fmt.Errorf("connection lost"))

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.WriteBack(context.Background(), "geo.poi", "42", &geocode.Result{
		Matched:   true,
		Latitude:  25.77,
		Longitude: -80.19,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "write back")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_MarkCompleteError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Claim phase.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(1, "geo.poi", "42", "100 Main St, Miami, FL"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{1}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	// markComplete UPDATE fails -- function just logs, does not return error.
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(1, pgxmock.AnyArg()).
		WillReturnError(fmt.Errorf("connection lost"))

	gc := &mockGeocodeClient{
		result: &geocode.Result{
			Matched:   true,
			Latitude:  25.77,
			Longitude: -80.19,
			Source:    "tiger",
		},
	}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	// ProcessBatch still succeeds; the markComplete error is only logged.
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow("not_an_int", "geo.poi", "42", "100 Main St"),
		)
	mock.ExpectRollback()

	q := NewGeocodeQueue(mock, nil, 100)
	_, err = q.ProcessBatch(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan row")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_MarkProcessingError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(1, "geo.poi", "42", "100 Main St"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{1}).
		WillReturnError(fmt.Errorf("mark processing failed"))
	mock.ExpectRollback()

	q := NewGeocodeQueue(mock, nil, 100)
	_, err = q.ProcessBatch(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark processing")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_CommitClaimError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(1, "geo.poi", "42", "100 Main St"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{1}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit().WillReturnError(fmt.Errorf("commit failed"))

	q := NewGeocodeQueue(mock, nil, 100)
	_, err = q.ProcessBatch(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit claim")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueBatch_BeginError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	q := NewGeocodeQueue(mock, nil, 100)
	err = q.EnqueueBatch(context.Background(), "geo.poi", []QueueItem{
		{SourceID: "1", Address: "100 Main St"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessBatch_MarkFailedError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Claim phase.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, source_table, source_id, address FROM geo.geocode_queue`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "source_table", "source_id", "address"}).
				AddRow(5, "geo.poi", "99", "Bad Address"),
		)
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs([]int{5}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	// markFailed UPDATE fails -- function just logs, does not return error.
	mock.ExpectExec(`UPDATE geo.geocode_queue`).
		WithArgs(5, "geocode error").
		WillReturnError(fmt.Errorf("connection lost"))

	gc := &mockGeocodeClient{err: fmt.Errorf("geocode error")}
	q := NewGeocodeQueue(mock, gc, 100)
	n, err := q.ProcessBatch(context.Background())

	// ProcessBatch still succeeds; the markFailed error is only logged.
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, mock.ExpectationsWereMet())
}
