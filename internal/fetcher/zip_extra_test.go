package fetcher

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractZIPFile_InvalidArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notazip.zip")
	require.NoError(t, os.WriteFile(path, []byte("not a zip"), 0o644))

	destDir := t.TempDir()
	_, err := ExtractZIPFile(path, "file.txt", destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip: open archive")
}

func TestExtractZIPSingle_InvalidArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notazip.zip")
	require.NoError(t, os.WriteFile(path, []byte("not a zip"), 0o644))

	destDir := t.TempDir()
	_, err := ExtractZIPSingle(path, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip: open archive")
}

func TestExtractZIPSingle_OnlyDirectories(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "dirs.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	// Add only directory entries
	_, err = w.Create("dir1/")
	require.NoError(t, err)
	_, err = w.Create("dir2/")
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	_, err = ExtractZIPSingle(zipPath, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly 1 file, got 0")
}

func TestExtractZIP_DestDirReadOnly(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"file.txt": "content",
	})

	destDir := t.TempDir()
	// Make the destination read-only
	require.NoError(t, os.Chmod(destDir, 0o555))
	defer os.Chmod(destDir, 0o755)

	_, err := ExtractZIP(zipPath, destDir)
	require.Error(t, err)
}

func TestExtractZIPFile_DestDirReadOnly(t *testing.T) {
	zipPath := createTestZIP(t, map[string]string{
		"file.txt": "content",
	})

	destDir := t.TempDir()
	require.NoError(t, os.Chmod(destDir, 0o555))
	defer os.Chmod(destDir, 0o755)

	_, err := ExtractZIPFile(zipPath, "file.txt", destDir)
	require.Error(t, err)
}

func TestExtractZIPSingle_ZipSlipPrevention(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "malicious.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	fw, err := w.Create("../../../etc/passwd")
	require.NoError(t, err)
	fw.Write([]byte("malicious"))
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	_, err = ExtractZIPSingle(zipPath, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip slip")
}

func TestExtractZIP_NestedSubdirectories(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "nested.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)

	// Create deeply nested file (parent dirs should be auto-created)
	fw, err := w.Create("a/b/c/deep.txt")
	require.NoError(t, err)
	fw.Write([]byte("deep content"))

	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	extracted, err := ExtractZIP(zipPath, destDir)
	require.NoError(t, err)
	assert.Len(t, extracted, 1)

	data, err := os.ReadFile(filepath.Join(destDir, "a", "b", "c", "deep.txt"))
	require.NoError(t, err)
	assert.Equal(t, "deep content", string(data))
}

func TestExtractZIP_EmptyArchive(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "empty.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	extracted, err := ExtractZIP(zipPath, destDir)
	require.NoError(t, err)
	assert.Empty(t, extracted)
}

func TestExtractZIPSingle_EmptyArchive(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "empty.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	_, err = ExtractZIPSingle(zipPath, destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly 1 file, got 0")
}

func TestExtractZIPFile_ZipSlipPrevention(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "malicious.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	fw, err := w.Create("../../../etc/passwd")
	require.NoError(t, err)
	fw.Write([]byte("malicious"))
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	_, err = ExtractZIPFile(zipPath, "../../../etc/passwd", destDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip slip")
}

func TestExtractZIPSingle_WithDirectoryAndOneFile(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "mixed.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	// Add directory
	_, err = w.Create("subdir/")
	require.NoError(t, err)
	// Add one file
	fw, err := w.Create("subdir/data.txt")
	require.NoError(t, err)
	fw.Write([]byte("content"))
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	destDir := t.TempDir()
	path, err := ExtractZIPSingle(zipPath, destDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(destDir, "subdir", "data.txt"), path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "content", string(data))
}
