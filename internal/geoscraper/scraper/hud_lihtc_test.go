package scraper

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// zipCSV wraps CSV data in an in-memory ZIP archive containing a single .csv file.
func zipCSV(t *testing.T, filename string, csvData []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create(filename)
	require.NoError(t, err)
	_, err = f.Write(csvData)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func expectLihtcUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_lihtc_projects"}, lihtcCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

func TestHUDLihtc_Metadata(t *testing.T) {
	s := &HUDLihtc{}
	assert.Equal(t, "hud_lihtc", s.Name())
	assert.Equal(t, "geo.lihtc_projects", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestHUDLihtc_ShouldRun(t *testing.T) {
	s := &HUDLihtc{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestHUDLihtc_Sync(t *testing.T) {
	csvData, err := os.ReadFile("testdata/hud_lihtc.csv")
	require.NoError(t, err)
	zipData := zipCSV(t, "LIHTCPUB.CSV", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(zipData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectLihtcUpsert(mock, 2)

	s := &HUDLihtc{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHUDLihtc_EmptyID(t *testing.T) {
	csvData := []byte("HUD_ID,PROJECT,PROJ_ST,PROJ_ZIP,LATITUDE,LONGITUDE,N_UNITS,LI_UNITS,YR_PIS\n" +
		",EMPTY ID PROJECT,TX,78701,30.2672,-97.7431,100,80,2005\n")
	zipData := zipCSV(t, "LIHTCPUB.CSV", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(zipData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDLihtc{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHUDLihtc_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDLihtc{baseURL: "http://127.0.0.1:1/lihtc.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestHUDLihtc_UpsertError(t *testing.T) {
	csvData, err := os.ReadFile("testdata/hud_lihtc.csv")
	require.NoError(t, err)
	zipData := zipCSV(t, "LIHTCPUB.CSV", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(zipData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &HUDLihtc{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestHUDLihtc_EmptyCSV(t *testing.T) {
	csvData := []byte("HUD_ID,PROJECT,PROJ_ST,PROJ_ZIP,LATITUDE,LONGITUDE,N_UNITS,LI_UNITS,YR_PIS\n")
	zipData := zipCSV(t, "LIHTCPUB.CSV", csvData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(zipData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDLihtc{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHUDLihtc_InvalidZIP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDLihtc{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract zip")
}
