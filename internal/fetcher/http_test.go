package fetcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newTestFetcher() *HTTPFetcher {
	return NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    5 * time.Second,
		MaxRetries: 3,
	})
}

func TestDownload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-agent", r.Header.Get("User-Agent"))
		w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	body, err := f.Download(context.Background(), srv.URL+"/data")
	require.NoError(t, err)
	defer body.Close()

	data, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func TestDownloadToFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("file content here"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	dir := t.TempDir()
	path := filepath.Join(dir, "out.txt")

	n, err := f.DownloadToFile(context.Background(), srv.URL+"/file", path)
	require.NoError(t, err)
	assert.Equal(t, int64(17), n)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "file content here", string(data))
}

func TestHeadETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method)
		w.Header().Set("ETag", `"abc123"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newTestFetcher()
	etag, err := f.HeadETag(context.Background(), srv.URL+"/resource")
	require.NoError(t, err)
	assert.Equal(t, `"abc123"`, etag)
}

func TestDownloadIfChanged_NotModified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"etag1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write([]byte("should not reach"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	body, etag, changed, err := f.DownloadIfChanged(context.Background(), srv.URL+"/res", `"etag1"`)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, body)
	assert.Equal(t, `"etag1"`, etag)
}

func TestDownloadIfChanged_Changed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag2"`)
		w.Write([]byte("new content"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	body, etag, changed, err := f.DownloadIfChanged(context.Background(), srv.URL+"/res", `"etag1"`)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, `"etag2"`, etag)

	data, err := io.ReadAll(body)
	body.Close()
	require.NoError(t, err)
	assert.Equal(t, "new content", string(data))
}

func TestRetryOnServerError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte("success"))
	}))
	defer srv.Close()

	// Use minimal backoff for test speed: override via a custom fetcher with short backoff
	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    5 * time.Second,
		MaxRetries: 3,
	})

	body, err := f.Download(context.Background(), srv.URL+"/retry")
	require.NoError(t, err)
	defer body.Close()

	data, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "success", string(data))
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    5 * time.Second,
		MaxRetries: 2,
	})

	_, err := f.Download(context.Background(), srv.URL+"/fail")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all retries exhausted")
}

func TestRateLimiting(t *testing.T) {
	var reqTimes []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqTimes = append(reqTimes, time.Now())
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	// Create a very restrictive rate limiter: 2 req/s
	limiters := map[string]*rate.Limiter{
		srv.Listener.Addr().String(): rate.NewLimiter(2, 1),
	}

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:    "test-agent",
		Timeout:      5 * time.Second,
		MaxRetries:   1,
		RateLimiters: limiters,
	})

	// The host in the URL will be the listener address
	ctx := context.Background()
	for range 3 {
		body, err := f.Download(ctx, srv.URL+"/limited")
		require.NoError(t, err)
		body.Close()
	}

	// With 2 req/s and burst=1, 3 requests should take at least ~1s
	require.GreaterOrEqual(t, len(reqTimes), 3)
	duration := reqTimes[len(reqTimes)-1].Sub(reqTimes[0])
	assert.GreaterOrEqual(t, duration.Milliseconds(), int64(500), "requests should be rate limited")
}
