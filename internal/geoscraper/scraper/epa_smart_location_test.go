package scraper

import (
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

func expectSLDUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_smart_location"}, sldCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

func TestEPASmartLocation_Metadata(t *testing.T) {
	s := &EPASmartLocation{}
	assert.Equal(t, "epa_smart_location", s.Name())
	assert.Equal(t, "geo.smart_location", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestEPASmartLocation_ShouldRun(t *testing.T) {
	s := &EPASmartLocation{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestEPASmartLocation_Sync(t *testing.T) {
	csvData, err := os.ReadFile("testdata/epa_smart_location.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectSLDUpsert(mock, 2)

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEPASmartLocation_EmptyGEOID(t *testing.T) {
	csvData := []byte("GEOID20,STATEFP,COUNTYFP,CBSA_Name,NatWalkInd,D3B,D1C,D1A,TotEmp,AutoOwn0\n" +
		",48,113,Dallas,15.2,42.5,8.3,4.1,5200,0.12\n")

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEPASmartLocation_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: "http://127.0.0.1:1/sld.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestEPASmartLocation_UpsertError(t *testing.T) {
	csvData, err := os.ReadFile("testdata/epa_smart_location.csv")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestEPASmartLocation_EmptyCSV(t *testing.T) {
	csvData := []byte("GEOID20,STATEFP,COUNTYFP,CBSA_Name,NatWalkInd,D3B,D1C,D1A,TotEmp,AutoOwn0\n")

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEPASmartLocation_ReadHeaderError(t *testing.T) {
	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": {},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read header")
}

func TestEPASmartLocation_ExtractError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip file"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestEPASmartLocation_NoCSVInZIP(t *testing.T) {
	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"readme.txt": []byte("no csv here"),
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SLD CSV not found")
}

func TestFindSLDCSV(t *testing.T) {
	got := findSLDCSV([]string{"/tmp/readme.txt", "/tmp/SmartLocationDatabaseV3.csv"})
	assert.Equal(t, "/tmp/SmartLocationDatabaseV3.csv", got)
}

func TestFindSLDCSV_Fallback(t *testing.T) {
	got := findSLDCSV([]string{"/tmp/readme.txt", "/tmp/data.csv"})
	assert.Equal(t, "/tmp/data.csv", got)
}

func TestFindSLDCSV_NotFound(t *testing.T) {
	got := findSLDCSV([]string{"/tmp/readme.txt", "/tmp/data.xlsx"})
	assert.Equal(t, "", got)
}

func TestEPASmartLocation_ReadRowError(t *testing.T) {
	csvData := []byte("GEOID20,STATEFP,COUNTYFP,CBSA_Name,NatWalkInd,D3B,D1C,D1A,TotEmp,AutoOwn0\n" +
		"481130101001,\"Broken Quote,113,Dallas,15.2,42.5,8.3,4.1,5200,0.12\n")

	tmpDir := t.TempDir()
	zipData := buildMultiZIP(t, tmpDir, map[string][]byte{
		"SmartLocationDatabaseV3.csv": csvData,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(zipData))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASmartLocation{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read row")
}
