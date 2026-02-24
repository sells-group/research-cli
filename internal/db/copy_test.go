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

func TestCopyFrom_EmptyRows(t *testing.T) {
	n, err := CopyFrom(context.TODO(), nil, "test_table", []string{"a", "b"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestCopyFrom_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectCopyFrom(pgx.Identifier{"test_table"}, []string{"a", "b"}).WillReturnResult(3)

	rows := [][]any{{1, "x"}, {2, "y"}, {3, "z"}}
	n, err := CopyFrom(context.Background(), mock, "test_table", []string{"a", "b"}, rows)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCopyFrom_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectCopyFrom(pgx.Identifier{"test_table"}, []string{"a", "b"}).WillReturnError(fmt.Errorf("copy failed"))

	rows := [][]any{{1, "x"}}
	_, err = CopyFrom(context.Background(), mock, "test_table", []string{"a", "b"}, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY INTO test_table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCopyFromSchema_EmptyRows(t *testing.T) {
	n, err := CopyFromSchema(context.TODO(), nil, "fed_data", "test_table", []string{"a"}, [][]any{})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestCopyFromSchema_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectCopyFrom(pgx.Identifier{"fed_data", "test_table"}, []string{"a", "b"}).WillReturnResult(5)

	rows := [][]any{{1, "x"}, {2, "y"}, {3, "z"}, {4, "w"}, {5, "v"}}
	n, err := CopyFromSchema(context.Background(), mock, "fed_data", "test_table", []string{"a", "b"}, rows)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCopyFromSchema_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectCopyFrom(pgx.Identifier{"fed_data", "test_table"}, []string{"a"}).WillReturnError(fmt.Errorf("permission denied"))

	rows := [][]any{{1}}
	_, err = CopyFromSchema(context.Background(), mock, "fed_data", "test_table", []string{"a"}, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY INTO fed_data.test_table")
	assert.NoError(t, mock.ExpectationsWereMet())
}
