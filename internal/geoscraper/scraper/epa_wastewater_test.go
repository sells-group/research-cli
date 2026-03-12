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

func TestWastewater_Metadata(t *testing.T) {
	s := &EPAWastewater{}
	assert.Equal(t, "epa_wastewater", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestWastewater_ShouldRun(t *testing.T) {
	s := &EPAWastewater{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently → should not run.
	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestWastewater_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/wastewater.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &EPAWastewater{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWastewater_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "CWP_NAME": "Good", "SOURCE_ID": "TX0001"}, "geometry": {"x": -95.0, "y": 30.0}},
			{"attributes": {"OBJECTID": 2, "CWP_NAME": "Bad", "SOURCE_ID": "TX0002"}, "geometry": null}
		],
		"exceededTransferLimit": false
	}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &EPAWastewater{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestWastewater_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/wastewater.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// BulkUpsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &EPAWastewater{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestEPAWastewater_Sync_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPAWastewater{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestWastewater_MissingSourceID(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "CWP_NAME": "Good", "SOURCE_ID": "TX0001"}, "geometry": {"x": -95.0, "y": 30.0}},
			{"attributes": {"OBJECTID": 2, "CWP_NAME": "NoID", "SOURCE_ID": ""}, "geometry": {"x": -96.0, "y": 31.0}}
		],
		"exceededTransferLimit": false
	}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &EPAWastewater{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWastewater_QueryError(t *testing.T) {
	// Point at a nonexistent server to trigger a download error.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPAWastewater{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}
