package fetcher

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestZIP(t *testing.T, files map[string]string) string {
	t.Helper()
	zipPath := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	w := zip.NewWriter(f)
	for name, content := range files {
		fw, err := w.Create(name)
		require.NoError(t, err)
		_, err = fw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return zipPath
}

func TestExtractZIP_MultiFile(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"file1.txt": "content one",
		"file2.txt": "content two",
		"file3.csv": "a,b,c",
	})

	destDir := t.TempDir()
	extracted, err := ExtractZIP(zipPath, destDir)
	require.NoError(t, err)
	assert.Len(t, extracted, 3)

	// Verify file contents
	for _, path := range extracted {
		_, err := os.Stat(path)
		require.NoError(t, err)
	}

	data, err := os.ReadFile(filepath.Join(destDir, "file1.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content one", string(data))

	data, err = os.ReadFile(filepath.Join(destDir, "file2.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content two", string(data))
}

func TestExtractZIPFile_Specific(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"a.txt": "aaa",
		"b.txt": "bbb",
		"c.txt": "ccc",
	})

	destDir := t.TempDir()
	path, err := ExtractZIPFile(zipPath, "b.txt", destDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(destDir, "b.txt"), path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "bbb", string(data))
}

func TestExtractZIPFile_NotFound(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"a.txt": "aaa",
	})

	destDir := t.TempDir()
	_, err := ExtractZIPFile(zipPath, "missing.txt", destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestExtractZIPSingle(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"only.csv": "x,y,z",
	})

	destDir := t.TempDir()
	path, err := ExtractZIPSingle(zipPath, destDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(destDir, "only.csv"), path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "x,y,z", string(data))
}

func TestExtractZIPSingle_MultipleFiles(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"a.txt": "aaa",
		"b.txt": "bbb",
	})

	destDir := t.TempDir()
	_, err := ExtractZIPSingle(zipPath, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly 1 file")
}

func TestExtractZIP_ZipSlipPrevention(t *testing.T) {
	// Create a ZIP with a malicious path
	zipPath := filepath.Join(t.TempDir(), "malicious.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	fw, err := w.Create("../../../etc/passwd")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("malicious")) //nolint:errcheck
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	_, err = ExtractZIP(zipPath, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip slip")
}

func TestExtractZIP_WithSubdirectory(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "nested.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)

	// Add a directory entry
	_, err = w.Create("subdir/")
	require.NoError(t, err)

	// Add a file in the subdirectory
	fw, err := w.Create("subdir/data.txt")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("nested content")) //nolint:errcheck

	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	extracted, err := ExtractZIP(zipPath, destDir)
	require.NoError(t, err)
	// Only the file should be in extracted (directories return empty string)
	assert.Len(t, extracted, 1)

	data, err := os.ReadFile(filepath.Join(destDir, "subdir", "data.txt"))
	require.NoError(t, err)
	assert.Equal(t, "nested content", string(data))
}

func TestExtractZIP_InvalidArchive(t *testing.T) {
	// Create a file that is not a ZIP
	path := filepath.Join(t.TempDir(), "notazip.zip")
	require.NoError(t, os.WriteFile(path, []byte("this is not a zip"), 0o644))

	destDir := t.TempDir()
	_, err := ExtractZIP(path, destDir)
	require.Error(t, err)
}
