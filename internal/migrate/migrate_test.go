package migrate

import (
	"os"
	"path/filepath"
	"testing"
)

// mockAtlasBinary creates a shell script that mimics atlas CLI.
// It writes TEST_STDOUT to stdout and exits 0 when TEST_STDOUT is set,
// otherwise writes TEST_STDERR to stderr and exits 1.
func mockAtlasBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "atlas")
	script := `#!/bin/bash
if [[ "$TEST_STDOUT" != "" ]]; then
  echo -n "$TEST_STDOUT"
  exit ${TEST_EXIT_CODE:-0}
fi
TEST_STDERR="${TEST_STDERR:-mock: no output configured}"
>&2 echo -n "$TEST_STDERR"
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec
		t.Fatalf("write mock atlas: %v", err)
	}
	return bin
}

func TestApply_EmptyURL(t *testing.T) {
	_, err := Apply(t.Context(), Options{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
	if got := err.Error(); got != "migrate: database URL is required" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestApply_Success(t *testing.T) {
	bin := mockAtlasBinary(t)
	// Atlas schema apply returns one JSON object per env.
	t.Setenv("TEST_STDOUT", `{"Changes":{"Applied":["CREATE TABLE test (id int)"]}}`)

	result, err := Apply(t.Context(), Options{
		URL:         "postgres://localhost/test",
		DevURL:      "postgres://localhost/dev",
		BinaryPath:  bin,
		AutoApprove: true,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
}

func TestApply_NoChanges(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDOUT", `{"Changes":{}}`)

	result, err := Apply(t.Context(), Options{
		URL:         "postgres://localhost/test",
		DevURL:      "postgres://localhost/dev",
		BinaryPath:  bin,
		AutoApprove: true,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0", result.Applied)
	}
	if result.Changes != "" {
		t.Errorf("Changes = %q, want empty", result.Changes)
	}
}

func TestApply_WithPendingChanges(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDOUT", `{"Changes":{"Pending":["ALTER TABLE foo ADD col int","CREATE INDEX idx"]}}`)

	result, err := Apply(t.Context(), Options{
		URL:         "postgres://localhost/test",
		DevURL:      "postgres://localhost/dev",
		BinaryPath:  bin,
		DryRun:      true,
		AutoApprove: true,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Changes == "" {
		t.Error("expected non-empty Changes for pending")
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (dry run)", result.Applied)
	}
}

func TestApply_CustomSchemas(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDOUT", `{"Changes":{"Applied":["CREATE TABLE x"]}}`)

	result, err := Apply(t.Context(), Options{
		URL:        "postgres://localhost/test",
		DevURL:     "postgres://localhost/dev",
		BinaryPath: bin,
		Schemas:    []string{"public"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
}

func TestApply_AtlasError(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDERR", "Error: connection refused")

	_, err := Apply(t.Context(), Options{
		URL:        "postgres://localhost/test",
		DevURL:     "postgres://localhost/dev",
		BinaryPath: bin,
	})
	if err == nil {
		t.Fatal("expected error from atlas")
	}
}

func TestApply_InvalidBinary(t *testing.T) {
	_, err := Apply(t.Context(), Options{
		URL:        "postgres://localhost/test",
		BinaryPath: "/nonexistent/atlas",
	})
	if err == nil {
		t.Fatal("expected error for invalid binary")
	}
}

func TestInspect_EmptyURL(t *testing.T) {
	_, err := Inspect(t.Context(), Options{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestInspect_Success(t *testing.T) {
	bin := mockAtlasBinary(t)
	hcl := `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = int }
}`
	t.Setenv("TEST_STDOUT", hcl)

	result, err := Inspect(t.Context(), Options{
		URL:        "postgres://localhost/test",
		BinaryPath: bin,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if result != hcl {
		t.Errorf("Inspect result = %q, want %q", result, hcl)
	}
}

func TestInspect_CustomSchemas(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDOUT", `schema "geo" {}`)

	result, err := Inspect(t.Context(), Options{
		URL:        "postgres://localhost/test",
		BinaryPath: bin,
		Schemas:    []string{"geo"},
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty inspect result")
	}
}

func TestInspect_AtlasError(t *testing.T) {
	bin := mockAtlasBinary(t)
	t.Setenv("TEST_STDERR", "Error: connection refused")

	_, err := Inspect(t.Context(), Options{
		URL:        "postgres://localhost/test",
		BinaryPath: bin,
	})
	if err == nil {
		t.Fatal("expected error from atlas")
	}
}

func TestInspect_InvalidBinary(t *testing.T) {
	_, err := Inspect(t.Context(), Options{
		URL:        "postgres://localhost/test",
		BinaryPath: "/nonexistent/atlas",
	})
	if err == nil {
		t.Fatal("expected error for invalid binary")
	}
}

func TestEmbeddedSchemaFS(t *testing.T) {
	// Verify key files exist in the embedded filesystem.
	expectedFiles := []string{
		"schema/extensions.sql",
		"schema/atlas.hcl",
		"schema/public.sql",
		"schema/fed_data.sql",
	}
	for _, f := range expectedFiles {
		data, err := schemaFS.ReadFile(f)
		if err != nil {
			t.Errorf("failed to read embedded file %s: %v", f, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("embedded file %s is empty", f)
		}
	}
}

func TestWriteSchemaToTemp(t *testing.T) {
	dir, err := writeSchemaToTemp()
	if err != nil {
		t.Fatalf("writeSchemaToTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) }) //nolint:errcheck

	if dir == "" {
		t.Fatal("writeSchemaToTemp returned empty dir")
	}

	// Verify atlas.hcl was written.
	data, err := os.ReadFile(filepath.Join(dir, "atlas.hcl"))
	if err != nil {
		t.Fatalf("read atlas.hcl: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("atlas.hcl is empty")
	}

	// Verify extensions.sql was written.
	data, err = os.ReadFile(filepath.Join(dir, "extensions.sql"))
	if err != nil {
		t.Fatalf("read extensions.sql: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("extensions.sql is empty")
	}

	// Verify consolidated schema files exist.
	for _, name := range []string{"public.sql", "fed_data.sql"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("missing %s: %v", name, err)
		} else if info.Size() == 0 {
			t.Errorf("%s is empty", name)
		}
	}
}

func TestBuildSrcURLs(t *testing.T) {
	urls := buildSrcURLs()
	expected := []string{
		"file://extensions.sql",
		"file://public.sql",
		"file://fed_data.sql",
	}
	if len(urls) != len(expected) {
		t.Fatalf("expected %d URLs, got %d", len(expected), len(urls))
	}
	for i, u := range urls {
		if u != expected[i] {
			t.Errorf("URL[%d] = %q, want %q", i, u, expected[i])
		}
	}
}

func TestApply_DefaultBinaryPath(t *testing.T) {
	// Exercises the binaryPath="" default. Atlas must be installed.
	if _, err := os.Stat("/opt/homebrew/bin/atlas"); err != nil {
		t.Skip("atlas not installed, skipping default binary test")
	}
	// Atlas will fail to connect, but the binary resolution succeeds and we hit the atlas error path.
	_, err := Apply(t.Context(), Options{
		URL:    "postgres://invalid:5432/nonexistent",
		DevURL: "postgres://invalid:5432/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error (no real DB)")
	}
}

func TestInspect_DefaultBinaryPath(t *testing.T) {
	if _, err := os.Stat("/opt/homebrew/bin/atlas"); err != nil {
		t.Skip("atlas not installed, skipping default binary test")
	}
	_, err := Inspect(t.Context(), Options{
		URL: "postgres://invalid:5432/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error (no real DB)")
	}
}

func TestCopyEmbeddedSchema(t *testing.T) {
	dir := t.TempDir()
	if err := copyEmbeddedSchema(dir); err != nil {
		t.Fatalf("copyEmbeddedSchema: %v", err)
	}

	// Verify key files.
	for _, name := range []string{"atlas.hcl", "extensions.sql", "public.sql", "fed_data.sql"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("missing %s: %v", name, err)
		} else if info.Size() == 0 {
			t.Errorf("%s is empty", name)
		}
	}
}

func TestOptions_Defaults(t *testing.T) {
	// Verify default schemas are empty (applied at runtime in Apply).
	var opts Options
	if len(opts.Schemas) != 0 {
		t.Errorf("expected empty schemas (defaults applied in Apply), got %v", opts.Schemas)
	}
}
