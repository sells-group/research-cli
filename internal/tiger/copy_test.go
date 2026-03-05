package tiger

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkLoad_PerState(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{
		Name:      "ADDR",
		Table:     "addr",
		PerCounty: true,
	}

	columns := []string{"tlid", "fromhn", "tohn"}
	rows := [][]any{
		{"123", "100", "200"},
		{"456", "300", "400"},
	}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid", "fromhn", "tohn"}).
		WillReturnResult(2)

	n, err := BulkLoad(context.Background(), mock, product, "FL", columns, rows, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkLoad_National(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{
		Name:     "STATE",
		Table:    "state_all",
		National: true,
		GeomType: "MULTIPOLYGON",
	}

	columns := []string{"statefp", "name", "the_geom"}
	rows := [][]any{
		{"12", "Florida", []byte("wkb-data")},
	}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "state_all"}, []string{"statefp", "name", "the_geom"}).
		WillReturnResult(1)

	n, err := BulkLoad(context.Background(), mock, product, "", columns, rows, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkLoad_EmptyRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "ADDR", Table: "addr", PerCounty: true}
	n, err := BulkLoad(context.Background(), mock, product, "FL", nil, nil, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestBulkLoad_BatchSplitting(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{
		Name:      "ADDR",
		Table:     "addr",
		PerCounty: true,
	}

	columns := []string{"tlid"}
	// 5 rows with batch size 2 = 3 COPY calls (2+2+1).
	rows := [][]any{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(2)
	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(2)
	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(1)

	n, err := BulkLoad(context.Background(), mock, product, "FL", columns, rows, 2)

	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTruncateTable_PerState(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "EDGES", Table: "edges", PerCounty: true}

	mock.ExpectExec(`TRUNCATE "tiger_data"."fl_edges"`).
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))

	err = TruncateTable(context.Background(), mock, product, "FL")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTruncateTable_National(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "STATE", Table: "state_all", National: true}

	mock.ExpectExec(`TRUNCATE "tiger_data"."state_all"`).
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))

	err = TruncateTable(context.Background(), mock, product, "")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTruncateTable_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "EDGES", Table: "edges", PerCounty: true}

	mock.ExpectExec(`TRUNCATE "tiger_data"."fl_edges"`).
		WillReturnError(assert.AnError)

	err = TruncateTable(context.Background(), mock, product, "FL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncate")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkLoad_CopyError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "ADDR", Table: "addr", PerCounty: true}

	columns := []string{"tlid"}
	rows := [][]any{{"123"}, {"456"}}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).
		WillReturnError(assert.AnError)

	n, err := BulkLoad(context.Background(), mock, product, "FL", columns, rows, 1000)
	require.Error(t, err)
	assert.Equal(t, int64(0), n)
	assert.Contains(t, err.Error(), "COPY")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkLoad_DefaultBatchSize(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "ADDR", Table: "addr", PerCounty: true}

	columns := []string{"tlid"}
	rows := [][]any{{"123"}}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).
		WillReturnResult(1)

	// Pass batchSize=0 to trigger the default assignment.
	n, err := BulkLoad(context.Background(), mock, product, "FL", columns, rows, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, mock.ExpectationsWereMet())
}
