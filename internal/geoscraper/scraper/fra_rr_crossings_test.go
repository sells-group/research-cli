package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestFRARRCrossings_Metadata(t *testing.T) {
	s := &FRARRCrossings{}
	assert.Equal(t, "fra_rr_crossings", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestFRARRCrossings_ShouldRun(t *testing.T) {
	s := &FRARRCrossings{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestFRARRCrossings_Sync(t *testing.T) {
	csvData, err := os.ReadFile("testdata/fra_rr_crossings.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFRARRCrossings_NullCoords(t *testing.T) {
	csvData := []byte("CROSSING,STREET,TYPEXING,LATITUDE,LONGITUDE\n" +
		"123456A,MAIN ST,Public,30.2672,-97.7431\n" +
		"789012B,BAD ST,Public,0,0\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestFRARRCrossings_Sync_NullCoords(t *testing.T) {
	csvData := []byte("CROSSING,STREET,TYPEXING,LATITUDE,LONGITUDE\n" +
		"999999Z,ZERO ST,Public,0,0\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFRARRCrossings_Sync_MissingID(t *testing.T) {
	csvData := []byte("CROSSING,STREET,TYPEXING,LATITUDE,LONGITUDE\n" +
		",NO ID CROSSING,Public,30.2672,-97.7431\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFRARRCrossings_UpsertError(t *testing.T) {
	csvData, err := os.ReadFile("testdata/fra_rr_crossings.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFRARRCrossings_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: "http://127.0.0.1:1/crossings.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestFRARRCrossings_Sync_EmptyCSV(t *testing.T) {
	csvData := []byte("CROSSING,STREET,TYPEXING,LATITUDE,LONGITUDE\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFRARRCrossings_ExtractError(t *testing.T) {
	// Serve a corrupt non-ZIP file so ExtractZIPSingle fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip file"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestFRARRCrossings_ReadHeaderError(t *testing.T) {
	// ZIP containing an empty CSV file (no header row).
	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", []byte(""))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read header")
}

func TestFRARRCrossings_ReadRowError(t *testing.T) {
	// CSV with malformed row to trigger read row error.
	csvData := []byte("CROSSING,STREET,TYPEXING,LATITUDE,LONGITUDE\n" +
		"123456A,\"Broken Quote,Public,30.0,-97.0\n")

	tmpDir := t.TempDir()
	zipData := buildZIP(t, tmpDir, "crossings.csv", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FRARRCrossings{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read row")
}

func TestFRARRCrossings_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &FRARRCrossings{baseURL: "http://127.0.0.1:1/crossings.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFRARRCrossings_DefaultURL(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &FRARRCrossings{} // empty baseURL triggers default URL
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}
