package sdk

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fedsync"
)

func TestStartSyncLog_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("cbp").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(42)))

	syncLog := fedsync.NewSyncLog(mock)
	a := &SyncLogActivities{SyncLog: syncLog}

	result, err := a.StartSyncLog(context.Background(), StartSyncLogParams{Name: "cbp"})
	require.NoError(t, err)
	require.Equal(t, int64(42), result.SyncID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStartSyncLog_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("cbp").
		WillReturnError(context.DeadlineExceeded)

	syncLog := fedsync.NewSyncLog(mock)
	a := &SyncLogActivities{SyncLog: syncLog}

	_, err = a.StartSyncLog(context.Background(), StartSyncLogParams{Name: "cbp"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "start sync log")
}

func TestCompleteSyncLog_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(100), pgxmock.AnyArg(), int64(42)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	syncLog := fedsync.NewSyncLog(mock)
	a := &SyncLogActivities{SyncLog: syncLog}

	err = a.CompleteSyncLog(context.Background(), CompleteSyncLogParams{
		SyncID:     42,
		RowsSynced: 100,
		Metadata:   map[string]any{"etag": "abc"},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFailSyncLog_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs("connection timeout", int64(42)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	syncLog := fedsync.NewSyncLog(mock)
	a := &SyncLogActivities{SyncLog: syncLog}

	err = a.FailSyncLog(context.Background(), FailSyncLogParams{
		SyncID: 42,
		Error:  "connection timeout",
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
