package fetcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestDownloadIfChanged_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, _, _, err := f.DownloadIfChanged(context.Background(), srv.URL+"/res", `"etag1"`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status 403")
}

func TestDownloadIfChanged_NoETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No If-None-Match header should be set when etag is empty
		assert.Empty(t, r.Header.Get("If-None-Match"))
		w.Header().Set("ETag", `"new-etag"`)
		w.Write([]byte("content"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	body, etag, changed, err := f.DownloadIfChanged(context.Background(), srv.URL+"/res", "")
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, `"new-etag"`, etag)
	data, _ := io.ReadAll(body)
	body.Close()
	assert.Equal(t, "content", string(data))
}

func TestHeadETag_NoETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method)
		w.WriteHeader(http.StatusOK)
		// No ETag header
	}))
	defer srv.Close()

	f := newTestFetcher()
	etag, err := f.HeadETag(context.Background(), srv.URL+"/resource")
	require.NoError(t, err)
	assert.Empty(t, etag)
}

func TestLimiterFor_UnknownHost(t *testing.T) {
	f := newTestFetcher()
	lim := f.limiterFor("https://unknown-host.com/path")
	assert.NotNil(t, lim)
	// Default limiter allows 20 req/s
	assert.InDelta(t, 20.0, float64(lim.Limit()), 0.001)
}

func TestLimiterFor_InvalidURL(t *testing.T) {
	f := newTestFetcher()
	lim := f.limiterFor("://invalid-url")
	assert.NotNil(t, lim)
}

func TestDownload_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.Download(context.Background(), srv.URL+"/forbidden")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status 403")
}

func TestDownloadToFile_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.DownloadToFile(context.Background(), srv.URL+"/notfound", "/tmp/out.txt")
	require.Error(t, err)
}

func TestDefaultRateLimiters(t *testing.T) {
	limiters := DefaultRateLimiters()
	assert.Contains(t, limiters, "efts.sec.gov")
	assert.Contains(t, limiters, "www.sec.gov")
	assert.Contains(t, limiters, "data.sec.gov")
	assert.Contains(t, limiters, "api.sam.gov")
}

func TestNewHTTPFetcher_Defaults(t *testing.T) {
	f := NewHTTPFetcher(HTTPOptions{})
	assert.Equal(t, "research-cli/1.0", f.opts.UserAgent)
	assert.Equal(t, 30*time.Second, f.opts.Timeout)
	assert.Equal(t, 3, f.opts.MaxRetries)
}

func TestDownload_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.Download(ctx, srv.URL+"/data")
	require.Error(t, err)
}

// --- AdaptiveLimiter Tests ---

func TestAdaptiveLimiter_OnSuccess_IncreasesRate(t *testing.T) {
	lim := NewAdaptiveLimiter(10, 10) // 10 req/s initial

	lim.OnSuccess()
	assert.InDelta(t, 12.0, float64(lim.Limit()), 0.1) // 10 * 1.2 = 12

	lim.OnSuccess()
	assert.InDelta(t, 14.4, float64(lim.Limit()), 0.1) // 12 * 1.2 = 14.4
}

func TestAdaptiveLimiter_OnRateLimit_DecreasesRate(t *testing.T) {
	lim := NewAdaptiveLimiter(10, 10) // 10 req/s initial

	lim.OnRateLimit()
	assert.InDelta(t, 5.0, float64(lim.Limit()), 0.1) // 10 * 0.5 = 5

	lim.OnRateLimit()
	assert.InDelta(t, 2.5, float64(lim.Limit()), 0.1) // 5 * 0.5 = 2.5
}

func TestAdaptiveLimiter_OnSuccess_CapsAt2x(t *testing.T) {
	lim := NewAdaptiveLimiter(10, 10) // max = 20

	// Call OnSuccess many times to exceed 2x
	for range 20 {
		lim.OnSuccess()
	}

	// Should be capped at 2x initial = 20
	assert.InDelta(t, 20.0, float64(lim.Limit()), 0.1)
}

func TestAdaptiveLimiter_OnRateLimit_FloorAtQuarter(t *testing.T) {
	lim := NewAdaptiveLimiter(10, 10) // min = 2.5

	// Call OnRateLimit many times
	for range 10 {
		lim.OnRateLimit()
	}

	// Should be floored at initial/4 = 2.5
	assert.InDelta(t, 2.5, float64(lim.Limit()), 0.1)
}

func TestAdaptiveLimiter_Wait(t *testing.T) {
	lim := NewAdaptiveLimiter(1000, 10) // Very high rate for quick test
	err := lim.Wait(context.Background())
	assert.NoError(t, err)
}

func TestAdaptiveLimiter_Wait_ContextCancelled(t *testing.T) {
	lim := NewAdaptiveLimiter(0.001, 0) // Very low rate, 0 burst
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := lim.Wait(ctx)
	assert.Error(t, err)
}

func TestDoWithRetry_429_AdaptiveBackoff(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	// Create fetcher with adaptive limiter for the test server host
	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    10 * time.Second,
		MaxRetries: 3,
	})

	// Get the adaptive limiter for the server host and replace it
	// Since the server URL host won't match any default adaptive limiters,
	// we need to add one manually
	u, _ := url.Parse(srv.URL)
	f.adaptiveLimiters[u.Host] = NewAdaptiveLimiter(100, 100) // high rate for test speed

	initialRate := f.adaptiveLimiters[u.Host].Limit()

	body, err := f.Download(context.Background(), srv.URL+"/data")
	require.NoError(t, err)
	defer body.Close()

	data, _ := io.ReadAll(body)
	assert.Equal(t, "ok", string(data))
	assert.Equal(t, int32(3), attempts.Load())

	// Adaptive limiter should have decreased from 429s then increased from success
	// After 2x OnRateLimit and 1x OnSuccess: 100 → 50 → 25 → 30
	currentRate := f.adaptiveLimiters[u.Host].Limit()
	assert.Less(t, float64(currentRate), float64(initialRate))
}

func TestHTTPTransport_PoolingConfig(t *testing.T) {
	f := NewHTTPFetcher(HTTPOptions{UserAgent: "test"})
	transport, ok := f.client.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 10, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 20, transport.MaxConnsPerHost)
}

func TestDefaultAdaptiveLimiters(t *testing.T) {
	limiters := DefaultAdaptiveLimiters()
	assert.Contains(t, limiters, "efts.sec.gov")
	assert.Contains(t, limiters, "www.sec.gov")
	assert.Contains(t, limiters, "data.sec.gov")
	assert.Contains(t, limiters, "api.sam.gov")

	// Verify initial rates
	assert.InDelta(t, 10.0, float64(limiters["efts.sec.gov"].Limit()), 0.1)
	assert.InDelta(t, 5.0, float64(limiters["api.sam.gov"].Limit()), 0.1)
}

func TestAdaptiveLimiterFor_KnownHost(t *testing.T) {
	f := NewHTTPFetcher(HTTPOptions{UserAgent: "test"})
	lim := f.adaptiveLimiterFor("https://data.sec.gov/submissions/CIK0001.json")
	assert.NotNil(t, lim)
}

func TestAdaptiveLimiterFor_UnknownHost(t *testing.T) {
	f := NewHTTPFetcher(HTTPOptions{UserAgent: "test"})
	lim := f.adaptiveLimiterFor("https://example.com/data")
	assert.Nil(t, lim) // No adaptive limiter for unknown hosts
}
