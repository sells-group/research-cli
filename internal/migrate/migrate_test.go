package migrate

import (
	"io/fs"
	"strings"
	"testing"
)

func TestApply_EmptyURL(t *testing.T) {
	_, err := Apply(t.Context(), Options{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
	if got := err.Error(); got != "migrate: database URL is required" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestStatus_EmptyURL(t *testing.T) {
	err := Status(t.Context(), Options{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestEmbeddedMigrationsFS(t *testing.T) {
	// Verify the embedded filesystem contains the expected migration files.
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("failed to read embedded migrations dir: %v", err)
	}

	if len(entries) < 4 {
		t.Errorf("expected at least 4 migration files, got %d", len(entries))
	}

	expectedFiles := []string{
		"00001_extensions.sql",
		"00002_public_schema.sql",
		"00003_fed_data_schema.sql",
		"00004_geo_schema.sql",
	}
	for _, want := range expectedFiles {
		data, err := migrationsFS.ReadFile("migrations/" + want)
		if err != nil {
			t.Errorf("missing migration file %s: %v", want, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("migration file %s is empty", want)
		}
	}
}

func TestMigrationsHaveGooseAnnotations(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			t.Errorf("read %s: %v", entry.Name(), err)
			continue
		}

		content := string(data)
		if !strings.Contains(content, "-- +goose Up") {
			t.Errorf("%s missing '-- +goose Up' annotation", entry.Name())
		}
		if !strings.Contains(content, "-- +goose Down") {
			t.Errorf("%s missing '-- +goose Down' annotation", entry.Name())
		}
	}
}

func TestMigrationsUseIfNotExists(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			t.Errorf("read %s: %v", entry.Name(), err)
			continue
		}

		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			// Skip comments and goose directives.
			if strings.HasPrefix(trimmed, "--") || trimmed == "" {
				continue
			}
			// CREATE TABLE must have IF NOT EXISTS.
			if strings.HasPrefix(trimmed, "CREATE TABLE ") && !strings.Contains(trimmed, "IF NOT EXISTS") {
				t.Errorf("%s:%d: CREATE TABLE without IF NOT EXISTS: %s", entry.Name(), i+1, trimmed)
			}
			// CREATE INDEX must have IF NOT EXISTS.
			if (strings.HasPrefix(trimmed, "CREATE INDEX ") || strings.HasPrefix(trimmed, "CREATE UNIQUE INDEX ")) &&
				!strings.Contains(trimmed, "IF NOT EXISTS") {
				t.Errorf("%s:%d: CREATE INDEX without IF NOT EXISTS: %s", entry.Name(), i+1, trimmed)
			}
		}
	}
}

func TestOptions_Defaults(t *testing.T) {
	var opts Options
	if opts.URL != "" {
		t.Errorf("expected empty URL, got %q", opts.URL)
	}
	if opts.DryRun {
		t.Error("expected DryRun to be false by default")
	}
}
