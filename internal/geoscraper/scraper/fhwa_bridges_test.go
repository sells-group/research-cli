package scraper

import (
	"archive/zip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestFHWABridges_Metadata(t *testing.T) {
	s := &FHWABridges{}
	assert.Equal(t, "fhwa_bridges", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFHWABridges_ShouldRun(t *testing.T) {
	s := &FHWABridges{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

// buildZIP creates a ZIP archive containing a single file.
func buildZIP(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	zipPath := filepath.Join(dir, "test.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(f)
	entry, err := w.Create(name)
	require.NoError(t, err)
	_, err = entry.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
	data2, err := os.ReadFile(zipPath)
	require.NoError(t, err)
	return string(data2)
}

func TestFHWABridges_Sync(t *testing.T) {
	csvData, err := os.ReadFile("testdata/fhwa_bridges.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFHWABridges_NullCoords(t *testing.T) {
	csvData := []byte("STRUCTURE_NUMBER_008,FACILITY_CARRIED_007,YEAR_BUILT_027,DECK_AREA,LAT_016,LONG_017\n" +
		"4800001,I-35 NB,1975,150.5,30160320,097443060\n" +
		"4800002,BAD BRIDGE,1990,100.0,00000000,000000000\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestFHWABridges_UpsertError(t *testing.T) {
	csvData, err := os.ReadFile("testdata/fhwa_bridges.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFHWABridges_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: "http://127.0.0.1:1/nbi.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestNbiDMS(t *testing.T) {
	// 30°16'03.20" = 30 + 16/60 + 3.20/3600 ≈ 30.2676
	got := nbiDMS("30160320")
	assert.InDelta(t, 30.2676, got, 0.001)

	// Empty and zero
	assert.Equal(t, 0.0, nbiDMS(""))
	assert.Equal(t, 0.0, nbiDMS("00000000"))
}
