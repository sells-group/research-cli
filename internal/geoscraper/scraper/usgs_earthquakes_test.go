package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestUSGSEarthquakes_Metadata(t *testing.T) {
	s := &USGSEarthquakes{}
	assert.Equal(t, "usgs_earthquakes", s.Name())
	assert.Equal(t, "geo.earthquakes", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Monthly, s.Cadence())
}

func TestUSGSEarthquakes_ShouldRun(t *testing.T) {
	s := &USGSEarthquakes{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced this month → should not run.
	recent := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestUSGSEarthquakes_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/earthquakes.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 3 regions × 2 features = 6 rows total, all in one batch.
	expectEarthquakeBulkUpsert(mock, 6)

	s := &USGSEarthquakes{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(6), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUSGSEarthquakes_NullMagnitude(t *testing.T) {
	data := []byte(`{
		"features": [
			{
				"properties": {"mag": 3.1, "place": "Test", "time": 1709900000000, "status": "reviewed", "tsunami": 0, "sig": 100, "magType": "ml", "type": "earthquake"},
				"geometry": {"type": "Point", "coordinates": [-117.0, 35.0, 5.0]},
				"id": "ok1"
			},
			{
				"properties": {"mag": null, "place": "Bad", "time": 1709800000000, "status": "automatic", "tsunami": 0, "sig": 50, "magType": "ml", "type": "earthquake"},
				"geometry": {"type": "Point", "coordinates": [-118.0, 34.0, 10.0]},
				"id": "bad1"
			}
		]
	}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 3 regions × 1 valid feature = 3 rows.
	expectEarthquakeBulkUpsert(mock, 3)

	s := &USGSEarthquakes{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
}

func TestUSGSEarthquakes_EmptyResponse(t *testing.T) {
	data := []byte(`{"features": []}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &USGSEarthquakes{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestUSGSEarthquakes_FetchError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &USGSEarthquakes{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch continental")
}

// expectEarthquakeBulkUpsert sets up pgxmock expectations for a BulkUpsert into geo.earthquakes.
func expectEarthquakeBulkUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_earthquakes"}, earthquakeCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
