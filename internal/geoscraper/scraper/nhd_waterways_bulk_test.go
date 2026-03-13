package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestNHDWaterwaysBulk_Metadata(t *testing.T) {
	s := &NHDWaterwaysBulk{}
	assert.Equal(t, "nhd_waterways_bulk", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestNHDWaterwaysBulk_ShouldRun(t *testing.T) {
	s := &NHDWaterwaysBulk{}
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestNHDWaterwaysBulk_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	s := &NHDWaterwaysBulk{
		baseURL: "http://127.0.0.1:1/nhd",
		states:  []string{"TX"},
	}
	// With cancelled context, ctx.Done fires before download.
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err) // ctx cancelled
}

func TestNHDWaterwaysBulk_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	s := &NHDWaterwaysBulk{states: []string{"TX"}}
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestNHDWaterwaysBulk_BuildURL(t *testing.T) {
	s := &NHDWaterwaysBulk{}
	url := s.buildURL("TX")
	assert.Contains(t, url, "NHD_H_TX_State_GDB.zip")

	s2 := &NHDWaterwaysBulk{baseURL: "http://test.com"}
	url2 := s2.buildURL("TX")
	assert.Equal(t, "http://test.com/TX.zip", url2)
}

func TestNHDWaterwaysBulk_StateSkipOnError(t *testing.T) {
	// Server returns 404 for state downloads — should skip gracefully.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &NHDWaterwaysBulk{
		baseURL: srv.URL,
		states:  []string{"XX"}, // fake state
	}

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	// Download returns 404 → state is skipped gracefully → 0 rows.
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFindGDB(t *testing.T) {
	// No GDB directory exists → empty string.
	dir := t.TempDir()
	assert.Equal(t, "", findGDB(dir))
}

func TestStrValFromMap(t *testing.T) {
	row := []any{"hello", nil, 42.0}
	cols := map[string]int{"name": 0, "empty": 1, "num": 2}

	assert.Equal(t, "hello", strValFromMap(row, cols, "name"))
	assert.Equal(t, "", strValFromMap(row, cols, "empty"))
	assert.Equal(t, "42", strValFromMap(row, cols, "num"))
	assert.Equal(t, "", strValFromMap(row, cols, "missing"))
}
