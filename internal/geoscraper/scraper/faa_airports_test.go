package scraper

import (
	"archive/zip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestFAAAirports_Metadata(t *testing.T) {
	s := &FAAAirports{}
	assert.Equal(t, "faa_airports", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Monthly, s.Cadence())
}

func TestFAAAirports_ShouldRun(t *testing.T) {
	s := &FAAAirports{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	// fixedNow() is March 1 — a sync on March 1 is in the current month.
	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

// buildMultiZIP creates a ZIP archive containing multiple files.
func buildMultiZIP(t *testing.T, dir string, files map[string][]byte) string {
	t.Helper()
	zipPath := filepath.Join(dir, "test.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(f)
	for name, data := range files {
		entry, wErr := w.Create(name)
		require.NoError(t, wErr)
		_, wErr = entry.Write(data)
		require.NoError(t, wErr)
	}
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
	data, err := os.ReadFile(zipPath)
	require.NoError(t, err)
	return string(data)
}

func TestFAAAirports_Sync(t *testing.T) {
	csvData, err := os.ReadFile("testdata/faa_airports.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"APT_BASE.csv": csvData,
		"OTHER.csv":    []byte("dummy"),
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &FAAAirports{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFAAAirports_NullCoords(t *testing.T) {
	csvData := []byte("ARPT_ID,ARPT_NAME,SITE_TYPE_CODE,LAT_DECIMAL,LONG_DECIMAL\n" +
		"AUS,AUSTIN-BERGSTROM INTL,A,30.1945,-97.6699\n" +
		"BAD,BAD AIRPORT,A,0,0\n")

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"APT_BASE.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &FAAAirports{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestFAAAirports_UpsertError(t *testing.T) {
	csvData, err := os.ReadFile("testdata/faa_airports.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"APT_BASE.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FAAAirports{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFAAAirports_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FAAAirports{baseURL: "http://127.0.0.1:1/nasr.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}
