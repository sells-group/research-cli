package geospatial

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	// Replace global logger with a no-op to avoid nil pointer panics in tests.
	zap.ReplaceGlobals(zap.NewNop())
}

// geoMigrationFileNames returns the sorted list of migration filenames from the embedded FS.
func geoMigrationFileNames(t *testing.T) []string {
	t.Helper()
	entries, err := fs.ReadDir(geoMigrationFS, "migrations")
	require.NoError(t, err)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// expectGeoAdvisoryLock adds the expected advisory lock acquire expectation.
func expectGeoAdvisoryLock(mock pgxmock.PgxPoolIface) {
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(pgxmock.NewResult("SELECT", 1))
}

// expectGeoAdvisoryUnlock adds the expected advisory unlock expectation.
func expectGeoAdvisoryUnlock(mock pgxmock.PgxPoolIface) {
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(pgxmock.NewResult("SELECT", 1))
}

func TestMigrate_FreshDB(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := geoMigrationFileNames(t)

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	// ensureGeoMigrationTable: CREATE SCHEMA + TABLE
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))

	// appliedGeoMigrations: returns empty set
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	// Each migration: Exec SQL then INSERT into schema_migrations
	for _, name := range names {
		mock.ExpectExec(".*").WillReturnResult(pgxmock.NewResult("EXEC", 0))
		mock.ExpectExec("INSERT INTO geo.schema_migrations").
			WithArgs(name).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}

	// Advisory unlock
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_AllAlreadyApplied(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := geoMigrationFileNames(t)

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	// ensureGeoMigrationTable
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))

	// appliedGeoMigrations: return all filenames
	appliedRows := pgxmock.NewRows([]string{"filename"})
	for _, name := range names {
		appliedRows.AddRow(name)
	}
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(appliedRows)

	// No Exec calls expected for migrations

	// Advisory unlock
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_EnsureTableError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").
		WillReturnError(fmt.Errorf("permission denied"))

	// Advisory unlock (deferred)
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensure migration table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_QueryAppliedError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnError(fmt.Errorf("relation does not exist"))

	// Advisory unlock (deferred)
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query applied migrations")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_ExecMigrationError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := geoMigrationFileNames(t)
	require.True(t, len(names) >= 1)

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	// First migration Exec fails
	mock.ExpectExec(".*").WillReturnError(fmt.Errorf("syntax error"))

	// Advisory unlock (deferred)
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply migration")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_RecordMigrationError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := geoMigrationFileNames(t)
	require.True(t, len(names) >= 1)

	// Advisory lock
	expectGeoAdvisoryLock(mock)

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	// First migration SQL succeeds
	mock.ExpectExec(".*").WillReturnResult(pgxmock.NewResult("EXEC", 0))
	// But recording it fails
	mock.ExpectExec("INSERT INTO geo.schema_migrations").
		WithArgs(names[0]).
		WillReturnError(fmt.Errorf("disk full"))

	// Advisory unlock (deferred)
	expectGeoAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record migration")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_AdvisoryLockError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("SELECT pg_advisory_lock").
		WillReturnError(fmt.Errorf("could not obtain lock"))

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire migration advisory lock")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnsureGeoMigrationTable_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))

	err = ensureGeoMigrationTable(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedGeoMigrations_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	applied, err := appliedGeoMigrations(context.Background(), mock)
	assert.NoError(t, err)
	assert.Empty(t, applied)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedGeoMigrations_WithEntries(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"filename"}).
		AddRow("001_geo_schema_init.sql").
		AddRow("002_geo_boundary_tables.sql")
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(rows)

	applied, err := appliedGeoMigrations(context.Background(), mock)
	assert.NoError(t, err)
	assert.True(t, applied["001_geo_schema_init.sql"])
	assert.True(t, applied["002_geo_boundary_tables.sql"])
	assert.False(t, applied["003_geo_poi_infrastructure.sql"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedGeoMigrations_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = appliedGeoMigrations(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query applied migrations")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedGeoMigrations_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"filename"}).
		AddRow(nil). // nil into string will fail
		RowError(0, fmt.Errorf("scan error"))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(rows)

	_, err = appliedGeoMigrations(context.Background(), mock)
	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
