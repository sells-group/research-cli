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

func TestFHWABridges_Sync_NullCoords(t *testing.T) {
	csvData := []byte("STRUCTURE_NUMBER_008,FACILITY_CARRIED_007,YEAR_BUILT_027,DECK_AREA,LAT_016,LONG_017\n" +
		"4800099,ZERO BRIDGE,2000,50.0,00000000,000000000\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFHWABridges_Sync_MissingID(t *testing.T) {
	csvData := []byte("STRUCTURE_NUMBER_008,FACILITY_CARRIED_007,YEAR_BUILT_027,DECK_AREA,LAT_016,LONG_017\n" +
		",NO ID BRIDGE,1985,200.0,30160320,097443060\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestFHWABridges_ExtractError(t *testing.T) {
	// Serve corrupt data that is not a valid ZIP.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip file"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestFHWABridges_ReadRowError(t *testing.T) {
	// CSV with malformed row to trigger read row error.
	csvData := []byte("STRUCTURE_NUMBER_008,FACILITY_CARRIED_007,YEAR_BUILT_027,DECK_AREA,LAT_016,LONG_017\n" +
		"4800001,\"Broken Quote,1975,150.5,30160320,097443060\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read row")
}

func TestNbiDMS(t *testing.T) {
	// 30°16'03.20" = 30 + 16/60 + 3.20/3600 ≈ 30.2676
	got := nbiDMS("30160320")
	assert.InDelta(t, 30.2676, got, 0.001)

	// Empty and zero
	assert.Equal(t, 0.0, nbiDMS(""))
	assert.Equal(t, 0.0, nbiDMS("00000000"))
}

func TestNbiDMS_Negative(t *testing.T) {
	got := nbiDMS("-30160320")
	assert.InDelta(t, -30.2676, got, 0.001)
}

func TestNbiDMS_Short(t *testing.T) {
	// Very short string that can't represent valid DMS — should still parse without panic.
	got := nbiDMS("12")
	// 12 < 1_000_000, so deg=0, mins=0, sec=12/100=0.12, result=0.12/3600 ≈ 0.0000333
	assert.InDelta(t, 0.0000333, got, 0.0001)
}

func TestFHWABridges_Sync_EmptyCSV(t *testing.T) {
	// CSV with header only — no data rows.
	csvData := []byte("STRUCTURE_NUMBER_008,FACILITY_CARRIED_007,YEAR_BUILT_027,DECK_AREA,LAT_016,LONG_017\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFHWABridges_ReadHeaderError(t *testing.T) {
	// ZIP with empty CSV file (no header row).
	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "nbi.csv", []byte(""))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWABridges{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read header")
}

func TestFHWABridges_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &FHWABridges{baseURL: "http://127.0.0.1:1/nbi.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFHWABridges_DefaultURL(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &FHWABridges{} // empty baseURL triggers default URL construction
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestNbiDMS_InvalidInput(t *testing.T) {
	assert.Equal(t, 0.0, nbiDMS("abc"))
	assert.Equal(t, 0.0, nbiDMS("  "))
}
