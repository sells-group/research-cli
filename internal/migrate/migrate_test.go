package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestApply_EmptyURL(t *testing.T) {
	err := Apply(t.Context(), "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
	if got := err.Error(); got != "migrate: database URL is required" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestStatus_EmptyURL(t *testing.T) {
	err := Status(t.Context(), "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestBaseline_EmptyURL(t *testing.T) {
	err := Baseline(t.Context(), "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestEmbeddedMigrationsFS(t *testing.T) {
	data, err := fs.ReadFile(migrationsFS, "migrations/00001_baseline.sql")
	if err != nil {
		t.Fatalf("failed to read embedded migration: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("embedded migration is empty")
	}

	// Verify it has goose annotations.
	content := string(data)
	if !contains(content, "-- +goose Up") {
		t.Error("migration missing '-- +goose Up' annotation")
	}
	if !contains(content, "-- +goose Down") {
		t.Error("migration missing '-- +goose Down' annotation")
	}
	if !contains(content, "IF NOT EXISTS") {
		t.Error("migration should use IF NOT EXISTS for idempotency")
	}
}

func TestEmbeddedMigrationsFS_ListFiles(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("failed to list migrations: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no migration files found")
	}

	found := false
	for _, e := range entries {
		if e.Name() == "00001_baseline.sql" {
			found = true
			break
		}
	}
	if !found {
		t.Error("00001_baseline.sql not found in embedded migrations")
	}
}

// --- needsBaseline tests (sqlmock) ---

func TestNeedsBaseline_GooseExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	// goose_db_version exists → no baseline needed, second query never runs.
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(
		sqlmock.NewRows([]string{"exists"}).AddRow(true),
	)

	result, err := needsBaseline(context.Background(), db)
	require.NoError(t, err)
	require.False(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNeedsBaseline_GooseMissing_CompaniesExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	// goose_db_version missing.
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(
		sqlmock.NewRows([]string{"exists"}).AddRow(false),
	)
	// companies exists → baseline needed.
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(
		sqlmock.NewRows([]string{"exists"}).AddRow(true),
	)

	result, err := needsBaseline(context.Background(), db)
	require.NoError(t, err)
	require.True(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNeedsBaseline_BothMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(
		sqlmock.NewRows([]string{"exists"}).AddRow(false),
	)
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(
		sqlmock.NewRows([]string{"exists"}).AddRow(false),
	)

	result, err := needsBaseline(context.Background(), db)
	require.NoError(t, err)
	require.False(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNeedsBaseline_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT EXISTS").WillReturnError(fmt.Errorf("connection refused"))

	_, err = needsBaseline(context.Background(), db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- Additional embedded FS tests ---

func TestEmbeddedMigrationsFS_MinimumCount(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 1, "expected at least 1 migration file")
}

func TestEmbeddedMigrationsFS_NamingConvention(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	require.NoError(t, err)

	pattern := regexp.MustCompile(`^\d{5}_\w+\.sql$`)
	for _, e := range entries {
		require.Truef(t, pattern.MatchString(e.Name()),
			"migration file %q does not match NNNNN_name.sql pattern", e.Name())
	}
}

func TestEmbeddedMigrationsFS_AllHaveGooseUp(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	require.NoError(t, err)

	for _, e := range entries {
		data, err := fs.ReadFile(migrationsFS, "migrations/"+e.Name())
		require.NoError(t, err)
		require.True(t, strings.Contains(string(data), "-- +goose Up"),
			"migration %q missing '-- +goose Up' annotation", e.Name())
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
