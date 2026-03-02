package analysis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLog(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	al := NewLog(mock)
	assert.NotNil(t, al)
}

// --- Start ---

func TestAnalysisLog_Start_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs("proximity_matrix").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(42)))

	al := NewLog(mock)
	id, err := al.Start(context.Background(), "proximity_matrix")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_Start_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs("proximity_matrix").
		WillReturnError(fmt.Errorf("disk full"))

	al := NewLog(mock)
	_, err = al.Start(context.Background(), "proximity_matrix")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start for proximity_matrix")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Complete ---

func TestAnalysisLog_Complete_WithResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(int64(100), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	al := NewLog(mock)
	result := &RunResult{
		RowsAffected: 100,
		Metadata:     map[string]any{"parcels": 5000},
	}
	err = al.Complete(context.Background(), 1, result)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_Complete_NilResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(5)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	al := NewLog(mock)
	err = al.Complete(context.Background(), 5, nil)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_Complete_ResultNoMetadata(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(int64(50), pgxmock.AnyArg(), int64(3)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	al := NewLog(mock)
	result := &RunResult{RowsAffected: 50}
	err = al.Complete(context.Background(), 3, result)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_Complete_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(1)).
		WillReturnError(fmt.Errorf("timeout"))

	al := NewLog(mock)
	err = al.Complete(context.Background(), 1, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "complete run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Fail ---

func TestAnalysisLog_Fail_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs("computation error", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	al := NewLog(mock)
	err = al.Fail(context.Background(), 7, "computation error")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_Fail_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs("computation error", int64(7)).
		WillReturnError(fmt.Errorf("connection reset"))

	al := NewLog(mock)
	err = al.Fail(context.Background(), 7, "computation error")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- LastSuccess ---

func TestAnalysisLog_LastSuccess_Found(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expected := time.Date(2026, 2, 15, 10, 30, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT started_at FROM geo.analysis_log").
		WithArgs("proximity_matrix").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}).AddRow(expected))

	al := NewLog(mock)
	ts, err := al.LastSuccess(context.Background(), "proximity_matrix")
	assert.NoError(t, err)
	require.NotNil(t, ts)
	assert.Equal(t, expected, *ts)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_LastSuccess_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT started_at FROM geo.analysis_log").
		WithArgs("proximity_matrix").
		WillReturnError(fmt.Errorf("no rows in result set"))

	al := NewLog(mock)
	ts, err := al.LastSuccess(context.Background(), "proximity_matrix")
	assert.NoError(t, err)
	assert.Nil(t, ts)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_LastSuccess_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT started_at FROM geo.analysis_log").
		WithArgs("proximity_matrix").
		WillReturnError(fmt.Errorf("connection refused"))

	al := NewLog(mock)
	_, err = al.LastSuccess(context.Background(), "proximity_matrix")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "last success")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- ListAll ---

func TestAnalysisLog_ListAll_MultipleEntries(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now().UTC()
	completed := now.Add(-time.Hour)
	meta := []byte(`{"parcels":5000}`)

	rows := pgxmock.NewRows([]string{"id", "analyzer", "status", "started_at", "completed_at", "rows_affected", "error", "metadata"}).
		AddRow(int64(2), "proximity_matrix", "complete", now, &completed, int64(100), (*string)(nil), meta).
		AddRow(int64(1), "parcel_scores", "failed", now.Add(-2*time.Hour), &completed, int64(0), strPtr("timeout"), nil)
	mock.ExpectQuery("SELECT id, analyzer, status, started_at, completed_at, rows_affected, error, metadata").
		WillReturnRows(rows)

	al := NewLog(mock)
	entries, err := al.ListAll(context.Background())
	assert.NoError(t, err)
	require.Len(t, entries, 2)

	assert.Equal(t, "proximity_matrix", entries[0].Analyzer)
	assert.Equal(t, "complete", entries[0].Status)
	assert.Equal(t, int64(100), entries[0].RowsAffected)
	assert.NotNil(t, entries[0].CompletedAt)
	assert.NotNil(t, entries[0].Metadata)
	assert.Equal(t, float64(5000), entries[0].Metadata["parcels"])
	assert.Empty(t, entries[0].Error)

	assert.Equal(t, "parcel_scores", entries[1].Analyzer)
	assert.Equal(t, "failed", entries[1].Status)
	assert.Equal(t, "timeout", entries[1].Error)
	assert.Nil(t, entries[1].Metadata)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_ListAll_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"id", "analyzer", "status", "started_at", "completed_at", "rows_affected", "error", "metadata"})
	mock.ExpectQuery("SELECT id, analyzer, status, started_at, completed_at, rows_affected, error, metadata").
		WillReturnRows(rows)

	al := NewLog(mock)
	entries, err := al.ListAll(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, entries)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_ListAll_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT id, analyzer, status, started_at, completed_at, rows_affected, error, metadata").
		WillReturnError(fmt.Errorf("table not found"))

	al := NewLog(mock)
	_, err = al.ListAll(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list all")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAnalysisLog_ListAll_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"id", "analyzer", "status", "started_at", "completed_at", "rows_affected", "error", "metadata"}).
		RowError(0, fmt.Errorf("scan error")).
		AddRow(int64(1), "proximity_matrix", "complete", time.Now(), nil, int64(0), nil, nil)
	mock.ExpectQuery("SELECT id, analyzer, status, started_at, completed_at, rows_affected, error, metadata").
		WillReturnRows(rows)

	al := NewLog(mock)
	_, err = al.ListAll(context.Background())
	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- helpers ---

func strPtr(s string) *string {
	return &s
}
