package analysis

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

// analysisMigrationFileNames returns the sorted list of migration filenames from the embedded FS.
func analysisMigrationFileNames(t *testing.T) []string {
	t.Helper()
	entries, err := fs.ReadDir(analysisMigrationFS, "migrations")
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

// expectAdvisoryLock adds the expected advisory lock acquire expectation.
func expectAdvisoryLock(mock pgxmock.PgxPoolIface) {
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(pgxmock.NewResult("SELECT", 1))
}

// expectAdvisoryUnlock adds the expected advisory unlock expectation.
func expectAdvisoryUnlock(mock pgxmock.PgxPoolIface) {
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(pgxmock.NewResult("SELECT", 1))
}

func TestMigrate_FreshDB(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := analysisMigrationFileNames(t)

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	for _, name := range names {
		mock.ExpectExec(".*").WillReturnResult(pgxmock.NewResult("EXEC", 0))
		mock.ExpectExec("INSERT INTO geo.schema_migrations").
			WithArgs(name).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}

	expectAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_AllAlreadyApplied(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := analysisMigrationFileNames(t)

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))

	appliedRows := pgxmock.NewRows([]string{"filename"})
	for _, name := range names {
		appliedRows.AddRow(name)
	}
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(appliedRows)

	expectAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_EnsureTableError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").
		WillReturnError(fmt.Errorf("permission denied"))
	expectAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensure migration table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_QueryAppliedError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnError(fmt.Errorf("relation does not exist"))
	expectAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query applied migrations")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_ExecMigrationError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := analysisMigrationFileNames(t)
	require.True(t, len(names) >= 1)

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	mock.ExpectExec(".*").WillReturnError(fmt.Errorf("syntax error"))
	expectAdvisoryUnlock(mock)

	err = Migrate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply migration")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrate_RecordMigrationError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	names := analysisMigrationFileNames(t)
	require.True(t, len(names) >= 1)

	expectAdvisoryLock(mock)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	mock.ExpectExec(".*").WillReturnResult(pgxmock.NewResult("EXEC", 0))
	mock.ExpectExec("INSERT INTO geo.schema_migrations").
		WithArgs(names[0]).
		WillReturnError(fmt.Errorf("disk full"))
	expectAdvisoryUnlock(mock)

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

func TestEnsureAnalysisMigrationTable_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").WillReturnResult(pgxmock.NewResult("CREATE", 0))

	err = ensureAnalysisMigrationTable(context.Background(), mock)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedAnalysisMigrations_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnRows(pgxmock.NewRows([]string{"filename"}))

	applied, err := appliedAnalysisMigrations(context.Background(), mock)
	assert.NoError(t, err)
	assert.Empty(t, applied)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedAnalysisMigrations_WithEntries(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"filename"}).
		AddRow("100_analysis_log.sql").
		AddRow("101_parcel_proximity.sql")
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(rows)

	applied, err := appliedAnalysisMigrations(context.Background(), mock)
	assert.NoError(t, err)
	assert.True(t, applied["100_analysis_log.sql"])
	assert.True(t, applied["101_parcel_proximity.sql"])
	assert.False(t, applied["102_parcel_scores.sql"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedAnalysisMigrations_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = appliedAnalysisMigrations(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query applied migrations")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppliedAnalysisMigrations_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"filename"}).
		AddRow(nil).
		RowError(0, fmt.Errorf("scan error"))
	mock.ExpectQuery("SELECT filename FROM geo.schema_migrations").WillReturnRows(rows)

	_, err = appliedAnalysisMigrations(context.Background(), mock)
	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrationFiles_Exist(t *testing.T) {
	names := analysisMigrationFileNames(t)
	require.Len(t, names, 3)
	assert.Equal(t, "100_analysis_log.sql", names[0])
	assert.Equal(t, "101_parcel_proximity.sql", names[1])
	assert.Equal(t, "102_parcel_scores.sql", names[2])
}
