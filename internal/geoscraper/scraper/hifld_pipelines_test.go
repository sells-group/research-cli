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
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

func TestPipelines_Metadata(t *testing.T) {
	s := &HIFLDPipelines{}
	assert.Equal(t, "hifld_pipelines", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestPipelines_ShouldRun(t *testing.T) {
	s := &HIFLDPipelines{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestPipelines_PolylineCentroid(t *testing.T) {
	g := &arcgis.Geometry{
		Paths: [][][2]float64{
			{{-98.0, 29.0}, {-97.0, 30.0}, {-96.0, 31.0}},
		},
	}

	lat, lon := g.Centroid()
	// avg lat = (29+30+31)/3 = 30, avg lon = (-98-97-96)/3 = -97
	assert.InDelta(t, 30.0, lat, 0.001)
	assert.InDelta(t, -97.0, lon, 0.001)
}

func TestPipelines_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/pipelines.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &HIFLDPipelines{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPipelines_FallbackName(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 888}, "geometry": {"paths": [[[-98.0, 29.0], [-97.0, 30.0]]]}}
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

	s := &HIFLDPipelines{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestPipelines_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "Operator": "Good"}, "geometry": {"paths": [[[-98.0, 29.0], [-97.0, 30.0]]]}},
			{"attributes": {"OBJECTID": 2, "Operator": "Bad"}, "geometry": null}
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

	s := &HIFLDPipelines{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestPipelines_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/pipelines.json")
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

	s := &HIFLDPipelines{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestPipelines_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDPipelines{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}

func TestPipelines_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDPipelines{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}
