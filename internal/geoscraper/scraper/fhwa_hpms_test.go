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

func TestFHWAHPMS_Metadata(t *testing.T) {
	s := &FHWAHPMs{}
	assert.Equal(t, "fhwa_hpms", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFHWAHPMS_ShouldRun(t *testing.T) {
	s := &FHWAHPMs{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestFHWAHPMS_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/fhwa_hpms.json")
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

	s := &FHWAHPMs{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFHWAHPMS_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "state_code": 48, "route_id": "IH0035", "begin_poin": 0.0, "f_system": 1, "aadt": 150000}, "geometry": {"paths": [[[-97.0, 30.0], [-97.1, 30.1]]]}},
			{"attributes": {"OBJECTID": 2, "state_code": 6, "route_id": "US0101", "begin_poin": 10.5, "f_system": 2, "aadt": 45000}, "geometry": null}
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

	s := &FHWAHPMs{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestFHWAHPMS_EmptyRouteID(t *testing.T) {
	// Second feature has null route_id and null state_code — both should be skipped.
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "state_code": 48, "route_id": "IH0035", "begin_poin": 0.0, "f_system": 1, "aadt": 150000}, "geometry": {"paths": [[[-97.0, 30.0], [-97.1, 30.1]]]}},
			{"attributes": {"OBJECTID": 2, "state_code": null, "route_id": null, "begin_poin": null, "f_system": 2, "aadt": 45000}, "geometry": {"paths": [[[-122.0, 37.0], [-122.1, 37.1]]]}},
			{"attributes": {"OBJECTID": 3, "state_code": 6, "begin_poin": 10.5, "f_system": 2, "aadt": 45000}, "geometry": {"paths": [[[-122.0, 37.0], [-122.1, 37.1]]]}}
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

	s := &FHWAHPMs{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestFHWAHPMS_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/fhwa_hpms.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FHWAHPMs{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFHWAHPMS_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FHWAHPMs{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}
