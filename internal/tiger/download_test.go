package tiger

import (
	"archive/zip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownload_Success(t *testing.T) {
	// Create a test ZIP with a .shp file inside.
	zipContent := createTestZIP(t, map[string]string{
		"test.shp": "fake shapefile data",
		"test.dbf": "fake dbf data",
		"test.shx": "fake shx data",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipContent)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	shpPath, err := Download(context.Background(), srv.URL+"/tl_2024_12_edges.zip", destDir)

	require.NoError(t, err)
	assert.Contains(t, shpPath, ".shp")
	assert.FileExists(t, shpPath)
}

func TestDownload_Resumable(t *testing.T) {
	zipContent := createTestZIP(t, map[string]string{
		"test.shp": "fake shapefile data",
	})

	var callCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipContent)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	url := srv.URL + "/tl_2024_12_edges.zip"

	// First download.
	_, err := Download(context.Background(), url, destDir)
	require.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Second download should skip (ZIP already exists).
	_, err = Download(context.Background(), url, destDir)
	require.NoError(t, err)
	assert.Equal(t, 1, callCount) // no additional HTTP call
}

func TestDownload_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	_, err := Download(context.Background(), srv.URL+"/bad.zip", destDir)
	assert.Error(t, err)
}

func TestDownload_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		// Slow response
		select {}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	destDir := t.TempDir()
	_, err := Download(ctx, srv.URL+"/slow.zip", destDir)
	assert.Error(t, err)
}

func TestExtractZIP(t *testing.T) {
	files := map[string]string{
		"file1.txt": "content1",
		"file2.shp": "shapefile content",
	}
	zipContent := createTestZIP(t, files)

	// Write ZIP to temp file.
	zipPath := filepath.Join(t.TempDir(), "test.zip")
	require.NoError(t, os.WriteFile(zipPath, zipContent, 0o644))

	extractDir := filepath.Join(t.TempDir(), "extracted")
	require.NoError(t, os.MkdirAll(extractDir, 0o755))

	err := extractZIP(zipPath, extractDir)
	require.NoError(t, err)

	// Verify extracted files.
	for name, expectedContent := range files {
		data, readErr := os.ReadFile(filepath.Join(extractDir, name))
		require.NoError(t, readErr)
		assert.Equal(t, expectedContent, string(data))
	}
}

func TestFindFileByExt(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.shp"), []byte("shp"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.dbf"), []byte("dbf"), 0o644))

	shpPath, err := findFileByExt(dir, ".shp")
	require.NoError(t, err)
	assert.Contains(t, shpPath, "data.shp")

	_, err = findFileByExt(dir, ".prj")
	assert.Error(t, err)
}

// createTestZIP creates a ZIP file in memory with the given files.
func createTestZIP(t *testing.T, files map[string]string) []byte {
	t.Helper()

	tmpFile := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	w := zip.NewWriter(f)
	for name, content := range files {
		fw, createErr := w.Create(name)
		require.NoError(t, createErr)
		_, writeErr := fw.Write([]byte(content))
		require.NoError(t, writeErr)
	}
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	data, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	return data
}
