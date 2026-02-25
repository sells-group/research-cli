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
		Name:     "ADDR",
		Table:    "addr",
		National: false,
		Columns:  []string{"tlid", "fromhn", "tohn"},
		GeomType: "",
	}

	rows := [][]any{
		{"123", "100", "200"},
		{"456", "300", "400"},
	}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid", "fromhn", "tohn"}).
		WillReturnResult(2)

	n, err := BulkLoad(context.Background(), mock, product, "FL", rows, 1000)

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
		Columns:  []string{"statefp", "name"},
		GeomType: "MULTIPOLYGON",
	}

	rows := [][]any{
		{"12", "Florida", []byte("wkb-data")},
	}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "state_all"}, []string{"statefp", "name", "the_geom"}).
		WillReturnResult(1)

	n, err := BulkLoad(context.Background(), mock, product, "", rows, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkLoad_EmptyRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "ADDR", Table: "addr"}
	n, err := BulkLoad(context.Background(), mock, product, "FL", nil, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestBulkLoad_BatchSplitting(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{
		Name:     "ADDR",
		Table:    "addr",
		National: false,
		Columns:  []string{"tlid"},
		GeomType: "",
	}

	// 5 rows with batch size 2 = 3 COPY calls (2+2+1).
	rows := [][]any{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}

	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(2)
	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(2)
	mock.ExpectCopyFrom(pgx.Identifier{"tiger_data", "fl_addr"}, []string{"tlid"}).WillReturnResult(1)

	n, err := BulkLoad(context.Background(), mock, product, "FL", rows, 2)

	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTruncateTable_PerState(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	product := Product{Name: "EDGES", Table: "edges", National: false}

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
