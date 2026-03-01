package geospatial

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureStatePartitions_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	fips := []string{"48", "06"}
	for range fips {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))
	}

	created, err := EnsureStatePartitions(context.Background(), mock, "counties", fips)
	assert.NoError(t, err)
	assert.Equal(t, 2, created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnsureStatePartitions_InvalidTable(t *testing.T) {
	_, err := EnsureStatePartitions(context.Background(), nil, "bogus_table", []string{"48"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not partitionable")
}

func TestEnsureStatePartitions_InvalidFIPS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	_, err = EnsureStatePartitions(context.Background(), mock, "counties", []string{"ABC"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid FIPS code")
}

func TestEnsureStatePartitions_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnError(fmt.Errorf("permission denied"))

	_, err = EnsureStatePartitions(context.Background(), mock, "counties", []string{"48"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create partition")
}

func TestEnsureStatePartitions_CensusTracts(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	created, err := EnsureStatePartitions(context.Background(), mock, "census_tracts", []string{"36"})
	assert.NoError(t, err)
	assert.Equal(t, 1, created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnsureStatePartitions_Places(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	created, err := EnsureStatePartitions(context.Background(), mock, "places", []string{"12"})
	assert.NoError(t, err)
	assert.Equal(t, 1, created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnsureAllStatePartitions(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	for range USStateFIPS {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))
	}

	created, err := EnsureAllStatePartitions(context.Background(), mock, "counties")
	assert.NoError(t, err)
	assert.Equal(t, len(USStateFIPS), created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreatePartitionIndexes_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	fips := []string{"48", "06"}
	for range fips {
		// GIST index
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))
		// B-tree index
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))
	}

	err = CreatePartitionIndexes(context.Background(), mock, "counties", fips)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreatePartitionIndexes_InvalidTable(t *testing.T) {
	err := CreatePartitionIndexes(context.Background(), nil, "bogus", []string{"48"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not partitionable")
}

func TestCreatePartitionIndexes_GistError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
		WillReturnError(fmt.Errorf("disk full"))

	err = CreatePartitionIndexes(context.Background(), mock, "counties", []string{"48"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GIST index")
}

func TestCreatePartitionIndexes_BtreeError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// GIST index succeeds
	mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// B-tree index fails
	mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
		WillReturnError(fmt.Errorf("disk full"))

	err = CreatePartitionIndexes(context.Background(), mock, "counties", []string{"48"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "B-tree index")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT child.relname").
		WithArgs("counties_partitioned").
		WillReturnRows(pgxmock.NewRows([]string{"relname"}).
			AddRow("counties_partitioned_06").
			AddRow("counties_partitioned_48"))

	names, err := ListPartitions(context.Background(), mock, "counties")
	require.NoError(t, err)
	assert.Equal(t, []string{"counties_partitioned_06", "counties_partitioned_48"}, names)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT child.relname").
		WithArgs("counties_partitioned").
		WillReturnRows(pgxmock.NewRows([]string{"relname"}))

	names, err := ListPartitions(context.Background(), mock, "counties")
	require.NoError(t, err)
	assert.Empty(t, names)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_InvalidTable(t *testing.T) {
	_, err := ListPartitions(context.Background(), nil, "bogus")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not partitionable")
}

func TestListPartitions_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT child.relname").
		WithArgs("counties_partitioned").
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = ListPartitions(context.Background(), mock, "counties")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list partitions")
}

func TestEnsureStatePartitions_AlreadyExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// "already exists" error should be skipped gracefully.
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnError(fmt.Errorf("relation already exists"))

	created, err := EnsureStatePartitions(context.Background(), mock, "counties", []string{"48"})
	assert.NoError(t, err)
	assert.Equal(t, 0, created) // Skipped, not created.
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT child.relname").
		WithArgs("counties_partitioned").
		WillReturnRows(pgxmock.NewRows([]string{"relname"}).
			AddRow("counties_partitioned_48").
			RowError(0, fmt.Errorf("scan error")))

	_, err = ListPartitions(context.Background(), mock, "counties")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan partition name")
}

func TestCreatePartitionIndexes_InvalidFIPS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	err = CreatePartitionIndexes(context.Background(), mock, "counties", []string{"XY"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid FIPS code")
}

func TestValidateFIPS(t *testing.T) {
	tests := []struct {
		fips    string
		wantErr bool
	}{
		{"48", false},
		{"06", false},
		{"01", false},
		{"", true},
		{"1", true},
		{"ABC", true},
		{"A1", true},
		{"123", true},
	}

	for _, tt := range tests {
		t.Run(tt.fips, func(t *testing.T) {
			err := validateFIPS(tt.fips)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
