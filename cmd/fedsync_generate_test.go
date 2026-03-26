package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

func TestParseGenerateTargets(t *testing.T) {
	targets, err := parseGenerateTargets("docs, frontend")
	require.NoError(t, err)
	assert.True(t, targets[generateTargetDocs])
	assert.True(t, targets[generateTargetFrontend])
}

func TestParseGenerateTargets_Invalid(t *testing.T) {
	_, err := parseGenerateTargets("bogus")
	require.Error(t, err)
}

func TestSyncGeneratedFile_CheckMode(t *testing.T) {
	root := t.TempDir()
	chdirTempRoot(t, root)

	path := filepath.Join(root, "frontend", "src", "lib", "config")
	require.NoError(t, os.MkdirAll(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "datasets.generated.ts"), []byte("old"), 0o600))

	err := syncFrontendCatalog("new", true)
	require.Error(t, err)
}

func TestSyncSummaryFile_UpdatesMarkedBlock(t *testing.T) {
	root := t.TempDir()
	chdirTempRoot(t, root)

	path := filepath.Join(root, "README.md")
	content := "before\n" + dataset.SummaryBlockStart + "\nstale\n" + dataset.SummaryBlockEnd + "\nafter\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	require.NoError(t, syncReadmeSummary(dataset.RenderMarkdownSummary(nil), false))

	updated, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(updated), "## Live Fedsync Dataset Summary")
	assert.Contains(t, string(updated), "before\n")
	assert.Contains(t, string(updated), "\nafter\n")
}

func TestSyncSummaryFile_CheckMode(t *testing.T) {
	root := t.TempDir()
	chdirTempRoot(t, root)

	path := filepath.Join(root, "README.md")
	content := "before\n" + dataset.SummaryBlockStart + "\nstale\n" + dataset.SummaryBlockEnd + "\nafter\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	err := syncReadmeSummary(dataset.RenderMarkdownSummary(nil), true)
	require.Error(t, err)
}

func chdirTempRoot(t *testing.T, root string) {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(wd))
	})
}
