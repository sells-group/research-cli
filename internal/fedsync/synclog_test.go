package fedsync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSyncLog(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	sl := NewSyncLog(mock)
	assert.NotNil(t, sl)
}

// --- LastSuccess ---

func TestSyncLog_LastSuccess_Found(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expected := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("cbp").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}).AddRow(expected))

	sl := NewSyncLog(mock)
	ts, err := sl.LastSuccess(context.Background(), "cbp")
	assert.NoError(t, err)
	require.NotNil(t, ts)
	assert.Equal(t, expected, *ts)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_LastSuccess_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("cbp").
		WillReturnError(fmt.Errorf("no rows in result set"))

	sl := NewSyncLog(mock)
	ts, err := sl.LastSuccess(context.Background(), "cbp")
	assert.NoError(t, err)
	assert.Nil(t, ts)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_LastSuccess_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("cbp").
		WillReturnError(fmt.Errorf("connection refused"))

	sl := NewSyncLog(mock)
	_, err = sl.LastSuccess(context.Background(), "cbp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "last success")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Start ---

func TestSyncLog_Start_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("cbp").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(42)))

	sl := NewSyncLog(mock)
	id, err := sl.Start(context.Background(), "cbp")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_Start_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("cbp").
		WillReturnError(fmt.Errorf("disk full"))

	sl := NewSyncLog(mock)
	_, err = sl.Start(context.Background(), "cbp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start sync")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Complete ---

func TestSyncLog_Complete_WithResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(100), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	sl := NewSyncLog(mock)
	result := &SyncResult{
		RowsSynced: 100,
		Metadata:   map[string]any{"year": 2024},
	}
	err = sl.Complete(context.Background(), 1, result)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_Complete_NilResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(5)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	sl := NewSyncLog(mock)
	err = sl.Complete(context.Background(), 5, nil)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_Complete_ResultNoMetadata(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(50), pgxmock.AnyArg(), int64(3)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	sl := NewSyncLog(mock)
	result := &SyncResult{RowsSynced: 50}
	err = sl.Complete(context.Background(), 3, result)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_Complete_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(1)).
		WillReturnError(fmt.Errorf("timeout"))

	sl := NewSyncLog(mock)
	err = sl.Complete(context.Background(), 1, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "complete sync")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Fail ---

func TestSyncLog_Fail_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs("download failed", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	sl := NewSyncLog(mock)
	err = sl.Fail(context.Background(), 7, "download failed")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_Fail_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs("download failed", int64(7)).
		WillReturnError(fmt.Errorf("connection reset"))

	sl := NewSyncLog(mock)
	err = sl.Fail(context.Background(), 7, "download failed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail sync")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- ListAll ---

func TestSyncLog_ListAll_MultipleEntries(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now().UTC()
	completed := now.Add(-time.Hour)
	meta := []byte(`{"year":2024}`)

	rows := pgxmock.NewRows([]string{"id", "dataset", "status", "started_at", "completed_at", "rows_synced", "error", "metadata"}).
		AddRow(int64(2), "cbp", "complete", now, &completed, int64(100), (*string)(nil), meta).
		AddRow(int64(1), "qcew", "failed", now.Add(-2*time.Hour), &completed, int64(0), strPtr("timeout"), nil)
	mock.ExpectQuery("SELECT id, dataset, status, started_at, completed_at, rows_synced, error, metadata").
		WillReturnRows(rows)

	sl := NewSyncLog(mock)
	entries, err := sl.ListAll(context.Background())
	assert.NoError(t, err)
	require.Len(t, entries, 2)

	assert.Equal(t, "cbp", entries[0].Dataset)
	assert.Equal(t, "complete", entries[0].Status)
	assert.Equal(t, int64(100), entries[0].RowsSynced)
	assert.NotNil(t, entries[0].CompletedAt)
	assert.NotNil(t, entries[0].Metadata)
	assert.Equal(t, float64(2024), entries[0].Metadata["year"])
	assert.Empty(t, entries[0].Error)

	assert.Equal(t, "qcew", entries[1].Dataset)
	assert.Equal(t, "failed", entries[1].Status)
	assert.Equal(t, "timeout", entries[1].Error)
	assert.Nil(t, entries[1].Metadata)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_ListAll_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"id", "dataset", "status", "started_at", "completed_at", "rows_synced", "error", "metadata"})
	mock.ExpectQuery("SELECT id, dataset, status, started_at, completed_at, rows_synced, error, metadata").
		WillReturnRows(rows)

	sl := NewSyncLog(mock)
	entries, err := sl.ListAll(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, entries)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_ListAll_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT id, dataset, status, started_at, completed_at, rows_synced, error, metadata").
		WillReturnError(fmt.Errorf("table not found"))

	sl := NewSyncLog(mock)
	_, err = sl.ListAll(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list all")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncLog_ListAll_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return a row that will cause a scan error (wrong type for id)
	rows := pgxmock.NewRows([]string{"id", "dataset", "status", "started_at", "completed_at", "rows_synced", "error", "metadata"}).
		RowError(0, fmt.Errorf("scan error")).
		AddRow(int64(1), "cbp", "complete", time.Now(), nil, int64(0), nil, nil)
	mock.ExpectQuery("SELECT id, dataset, status, started_at, completed_at, rows_synced, error, metadata").
		WillReturnRows(rows)

	sl := NewSyncLog(mock)
	_, err = sl.ListAll(context.Background())
	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- helpers ---

func strPtr(s string) *string {
	return &s
}
