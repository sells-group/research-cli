package migrate

import (
	"io/fs"
	"testing"
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
