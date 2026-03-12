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

func TestRRCrossings_Metadata(t *testing.T) {
	s := &HIFLDRRCrossings{}
	assert.Equal(t, "hifld_rr_crossings", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestRRCrossings_ShouldRun(t *testing.T) {
	s := &HIFLDRRCrossings{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (same quarter) → should not run.
	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestRRCrossings_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/rr_crossings.json")
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

	s := &HIFLDRRCrossings{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRRCrossings_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "Crossing_ID": "Good"}, "geometry": {"x": -95.0, "y": 30.0}},
			{"attributes": {"OBJECTID": 2, "Crossing_ID": "Bad"}, "geometry": null}
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

	s := &HIFLDRRCrossings{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestRRCrossings_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/rr_crossings.json")
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

	s := &HIFLDRRCrossings{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestRRCrossings_EmptyResponse(t *testing.T) {
	data := []byte(`{"features": [], "exceededTransferLimit": false}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDRRCrossings{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestRRCrossings_QueryError(t *testing.T) {
	// Point at a nonexistent server to trigger a download error.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDRRCrossings{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}
