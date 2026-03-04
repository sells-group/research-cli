package tiger

import (
	"archive/zip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	// Use fast backoffs for testing so retries don't cause timeout.
	orig := retryBackoffs
	retryBackoffs = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	t.Cleanup(func() { retryBackoffs = orig })

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

func TestDownload_TabularProduct_DBFOnly(t *testing.T) {
	// Create a ZIP containing only a .dbf file (no .shp).
	// This simulates tabular products like ADDR/FEATNAMES.
	zipContent := createTestZIP(t, map[string]string{
		"test.dbf": "fake dbf data",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipContent)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	path, err := Download(context.Background(), srv.URL+"/tl_2024_12086_addr.zip", destDir)

	require.NoError(t, err)
	assert.Contains(t, path, ".dbf", "should fall back to .dbf when no .shp found")
	assert.FileExists(t, path)
}

func TestDownload_NotFoundError(t *testing.T) {
	// Use fast backoffs for testing.
	orig := retryBackoffs
	retryBackoffs = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	t.Cleanup(func() { retryBackoffs = orig })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	_, err := Download(context.Background(), srv.URL+"/notfound.zip", destDir)
	assert.Error(t, err)
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		msg      string
		expected bool
	}{
		{"download returned status 403", true},
		{"download returned status 429", true},
		{"download returned status 500", true},
		{"download returned status 503", true},
		{"not a valid ZIP", true},
		{"download returned status 404", false},
		{"connection refused", false},
	}
	for _, tc := range tests {
		t.Run(tc.msg, func(t *testing.T) {
			err := fmt.Errorf("%s", tc.msg)
			assert.Equal(t, tc.expected, isRetryable(err))
		})
	}
}

func TestValidateZIP_Valid(t *testing.T) {
	zipContent := createTestZIP(t, map[string]string{
		"test.shp": "fake shapefile data",
	})

	zipPath := filepath.Join(t.TempDir(), "valid.zip")
	require.NoError(t, os.WriteFile(zipPath, zipContent, 0o644))

	err := validateZIP(zipPath)
	assert.NoError(t, err)
}

func TestValidateZIP_Invalid(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "invalid.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("not zip content"), 0o644))

	err := validateZIP(zipPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ZIP magic bytes")
}

func TestValidateZIP_Empty(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "empty.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte{}, 0o644))

	err := validateZIP(zipPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read magic bytes")
}

func TestDownload_InvalidCachedZIP(t *testing.T) {
	// Create a valid ZIP that the server will return.
	zipContent := createTestZIP(t, map[string]string{
		"test.shp": "fake shapefile data",
		"test.dbf": "fake dbf data",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipContent)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	zipName := "tl_2024_12_edges.zip"

	// Pre-create an invalid ZIP at the expected path.
	invalidZipPath := filepath.Join(destDir, zipName)
	require.NoError(t, os.WriteFile(invalidZipPath, []byte("not a zip file"), 0o644))

	// Download should detect the invalid cached ZIP, re-download, and succeed.
	shpPath, err := Download(context.Background(), srv.URL+"/"+zipName, destDir)
	require.NoError(t, err)
	assert.Contains(t, shpPath, ".shp")
	assert.FileExists(t, shpPath)
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
