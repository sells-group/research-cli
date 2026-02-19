package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkUpsert_EmptyRows(t *testing.T) {
	n, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"id", "name"},
		ConflictKeys: []string{"id"},
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestBulkUpsert_NoColumns(t *testing.T) {
	_, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:        "fed_data.test",
		ConflictKeys: []string{"id"},
	}, [][]any{{1, "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no columns specified")
}

func TestBulkUpsert_NoConflictKeys(t *testing.T) {
	_, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:   "fed_data.test",
		Columns: []string{"id", "name"},
	}, [][]any{{1, "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no conflict keys specified")
}

func TestBulkUpsert_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_test"}, []string{"col1", "col2"}).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectCommit()

	rows := [][]any{{"a", 1}, {"b", 2}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	n, err := BulkUpsert(context.Background(), mock, cfg, rows)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_Success_ExplicitUpdateCols(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_test"}, []string{"id", "name", "value"}).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	rows := [][]any{{1, "a", 100}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"id", "name", "value"},
		ConflictKeys: []string{"id"},
		UpdateCols:   []string{"value"},
	}
	n, err := BulkUpsert(context.Background(), mock, cfg, rows)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_SimpleTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_mytable"}, []string{"id", "name"}).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	rows := [][]any{{1, "a"}}
	cfg := UpsertConfig{
		Table:        "mytable",
		Columns:      []string{"id", "name"},
		ConflictKeys: []string{"id"},
	}
	n, err := BulkUpsert(context.Background(), mock, cfg, rows)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_BeginError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	rows := [][]any{{"a", 1}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	_, err = BulkUpsert(context.Background(), mock, cfg, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_CreateTempError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnError(fmt.Errorf("permission denied"))
	mock.ExpectRollback()

	rows := [][]any{{"a", 1}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	_, err = BulkUpsert(context.Background(), mock, cfg, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create temp table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_CopyError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_test"}, []string{"col1", "col2"}).WillReturnError(fmt.Errorf("copy failed"))
	mock.ExpectRollback()

	rows := [][]any{{"a", 1}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	_, err = BulkUpsert(context.Background(), mock, cfg, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY into temp table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_InsertConflictError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_test"}, []string{"col1", "col2"}).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnError(fmt.Errorf("unique violation"))
	mock.ExpectRollback()

	rows := [][]any{{"a", 1}, {"b", 2}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	_, err = BulkUpsert(context.Background(), mock, cfg, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INSERT ON CONFLICT")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsert_CommitError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_test"}, []string{"col1", "col2"}).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectCommit().WillReturnError(fmt.Errorf("commit failed"))
	mock.ExpectRollback()

	rows := [][]any{{"a", 1}, {"b", 2}}
	cfg := UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"col1", "col2"},
		ConflictKeys: []string{"col1"},
	}
	_, err = BulkUpsert(context.Background(), mock, cfg, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit tx")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSanitizeTable(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", `"simple"`},
		{"fed_data.cbp_data", `"fed_data"."cbp_data"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeTable(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestQuoteAndJoin(t *testing.T) {
	result := quoteAndJoin([]string{"id", "name", "value"})
	assert.Equal(t, `"id", "name", "value"`, result)
}

func TestQuoteAndJoin_Single(t *testing.T) {
	result := quoteAndJoin([]string{"id"})
	assert.Equal(t, `"id"`, result)
}

// --- BulkUpsertMulti Tests ---

func TestBulkUpsertMulti_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Single transaction for both tables
	mock.ExpectBegin()

	// Table 1
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_table1"}, []string{"id", "name"}).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))

	// Table 2
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_table2"}, []string{"key", "value"}).WillReturnResult(3)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 3))

	mock.ExpectCommit()

	entries := []MultiUpsertEntry{
		{
			Config: UpsertConfig{Table: "fed_data.table1", Columns: []string{"id", "name"}, ConflictKeys: []string{"id"}},
			Rows:   [][]any{{1, "a"}, {2, "b"}},
		},
		{
			Config: UpsertConfig{Table: "fed_data.table2", Columns: []string{"key", "value"}, ConflictKeys: []string{"key"}},
			Rows:   [][]any{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}},
		},
	}

	results, err := BulkUpsertMulti(context.Background(), mock, entries)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), results["fed_data.table1"])
	assert.Equal(t, int64(3), results["fed_data.table2"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsertMulti_EmptyEntries(t *testing.T) {
	// All entries have 0 rows — no transaction should start
	entries := []MultiUpsertEntry{
		{Config: UpsertConfig{Table: "t1", Columns: []string{"id"}, ConflictKeys: []string{"id"}}},
		{Config: UpsertConfig{Table: "t2", Columns: []string{"id"}, ConflictKeys: []string{"id"}}},
	}

	results, err := BulkUpsertMulti(context.Background(), nil, entries) // nil pool is fine since no tx
	assert.NoError(t, err)
	assert.Empty(t, results)
}

func TestBulkUpsertMulti_MixedEmptyAndFull(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_active"}, []string{"id", "val"}).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	entries := []MultiUpsertEntry{
		{Config: UpsertConfig{Table: "fed_data.empty", Columns: []string{"id"}, ConflictKeys: []string{"id"}}}, // no rows
		{
			Config: UpsertConfig{Table: "fed_data.active", Columns: []string{"id", "val"}, ConflictKeys: []string{"id"}},
			Rows:   [][]any{{1, "x"}},
		},
	}

	results, err := BulkUpsertMulti(context.Background(), mock, entries)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), results["fed_data.active"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsertMulti_NoColumns(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	entries := []MultiUpsertEntry{
		{
			Config: UpsertConfig{Table: "t1", ConflictKeys: []string{"id"}}, // no columns!
			Rows:   [][]any{{1}},
		},
	}

	_, err = BulkUpsertMulti(context.Background(), mock, entries)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no columns specified")
}

func TestBulkUpsertMulti_NoConflictKeys(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	entries := []MultiUpsertEntry{
		{
			Config: UpsertConfig{Table: "t1", Columns: []string{"id", "name"}}, // no conflict keys!
			Rows:   [][]any{{1, "a"}},
		},
	}

	_, err = BulkUpsertMulti(context.Background(), mock, entries)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no conflict keys specified")
}

func TestBulkUpsertMulti_TransactionRollbackOnError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	// Table 1 succeeds
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_t1"}, []string{"id"}).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	// Table 2 fails on COPY
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_t2"}, []string{"id"}).WillReturnError(fmt.Errorf("copy failed"))
	mock.ExpectRollback()

	entries := []MultiUpsertEntry{
		{Config: UpsertConfig{Table: "fed_data.t1", Columns: []string{"id"}, ConflictKeys: []string{"id"}}, Rows: [][]any{{1}}},
		{Config: UpsertConfig{Table: "fed_data.t2", Columns: []string{"id"}, ConflictKeys: []string{"id"}}, Rows: [][]any{{2}}},
	}

	_, err = BulkUpsertMulti(context.Background(), mock, entries)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY into temp table")
	assert.NoError(t, mock.ExpectationsWereMet())
}
