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

func TestDoWithRetry_NetworkError(t *testing.T) {
	// Server that immediately closes connections to simulate network errors.
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			// Trigger a real request error by hijacking and closing the connection
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close() //nolint:errcheck
				return
			}
		}
		w.Write([]byte("ok")) //nolint:errcheck
	}))
	defer srv.Close()

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    2 * time.Second,
		MaxRetries: 3,
	})

	body, err := f.Download(context.Background(), srv.URL+"/net-err")
	require.NoError(t, err)
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "ok", string(data))
	assert.GreaterOrEqual(t, attempts.Load(), int32(3))
}

func TestDoWithRetry_AllNetworkErrors(t *testing.T) {
	// Server that always closes the connection
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			conn.Close() //nolint:errcheck
			return
		}
	}))
	defer srv.Close()

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    1 * time.Second,
		MaxRetries: 2,
	})

	_, err := f.Download(context.Background(), srv.URL+"/fail")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all retries exhausted")
}

func TestBackoff_MaxCap(t *testing.T) {
	f := newTestFetcher()
	ctx := context.Background()

	// With a high attempt number, backoff should be capped at 30s.
	// We just verify it doesn't block forever by using a short context timeout.
	ctxShort, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	f.backoff(ctxShort, 20) // attempt 20 -> 2^20 seconds would be huge without cap
	elapsed := time.Since(start)

	// Should return quickly due to context cancellation, not wait 30s
	assert.Less(t, elapsed, 1*time.Second)
}

func TestBackoff_ContextCancelled(t *testing.T) {
	f := newTestFetcher()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	f.backoff(ctx, 0)
	elapsed := time.Since(start)

	// Should return immediately since context is already done
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestDownload_InvalidURL(t *testing.T) {
	f := newTestFetcher()
	_, err := f.Download(context.Background(), "://invalid-url")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestDownloadToFile_CreateFileError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("data")) //nolint:errcheck
	}))
	defer srv.Close()

	f := newTestFetcher()
	// Use a path that can't be created (nonexistent parent directory)
	_, err := f.DownloadToFile(context.Background(), srv.URL+"/file", "/nonexistent/dir/file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create file")
}

func TestDownloadToFile_ReadOnlyDir(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("content")) //nolint:errcheck
	}))
	defer srv.Close()

	f := newTestFetcher()
	dir := t.TempDir()

	// Make directory read-only
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) //nolint:errcheck

	path := filepath.Join(dir, "out.txt")
	_, err := f.DownloadToFile(context.Background(), srv.URL+"/file", path)
	require.Error(t, err)
}

func TestHeadETag_InvalidURL(t *testing.T) {
	f := newTestFetcher()
	_, err := f.HeadETag(context.Background(), "://invalid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create head request")
}

func TestHeadETag_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newTestFetcher()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.HeadETag(ctx, srv.URL+"/res")
	require.Error(t, err)
}

func TestHeadETag_ServerError(t *testing.T) {
	// Use a server that closes the connection to trigger a network error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			conn.Close() //nolint:errcheck
		}
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.HeadETag(context.Background(), srv.URL+"/res")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "head request")
}

func TestHeadETag_RateLimiterCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Create a very slow rate limiter
	limiters := map[string]*rate.Limiter{
		srv.Listener.Addr().String(): rate.NewLimiter(rate.Every(10*time.Second), 0),
	}

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:    "test-agent",
		Timeout:      5 * time.Second,
		MaxRetries:   1,
		RateLimiters: limiters,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := f.HeadETag(ctx, srv.URL+"/res")
	require.Error(t, err)
}

func TestDownloadIfChanged_InvalidURL(t *testing.T) {
	f := newTestFetcher()
	_, _, _, err := f.DownloadIfChanged(context.Background(), "://invalid", "etag")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestDownloadIfChanged_RateLimiterCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("data")) //nolint:errcheck
	}))
	defer srv.Close()

	// Create a very slow rate limiter with 0 burst
	limiters := map[string]*rate.Limiter{
		srv.Listener.Addr().String(): rate.NewLimiter(rate.Every(10*time.Second), 0),
	}

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:    "test-agent",
		Timeout:      5 * time.Second,
		MaxRetries:   1,
		RateLimiters: limiters,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, _, _, err := f.DownloadIfChanged(ctx, srv.URL+"/res", "etag")
	require.Error(t, err)
}

func TestDownloadIfChanged_NetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			conn.Close() //nolint:errcheck
		}
	}))
	defer srv.Close()

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:  "test-agent",
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	})

	_, _, _, err := f.DownloadIfChanged(context.Background(), srv.URL+"/res", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download if changed")
}

func TestDoWithRetry_RateLimiterCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok")) //nolint:errcheck
	}))
	defer srv.Close()

	// Zero burst limiter - will block forever waiting for a token
	limiters := map[string]*rate.Limiter{
		srv.Listener.Addr().String(): rate.NewLimiter(rate.Every(10*time.Second), 0),
	}

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:    "test-agent",
		Timeout:      5 * time.Second,
		MaxRetries:   1,
		RateLimiters: limiters,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := f.Download(ctx, srv.URL+"/data")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limiter wait")
}

func TestLimiterFor_KnownHost(t *testing.T) {
	limiters := map[string]*rate.Limiter{
		"custom.host.com": rate.NewLimiter(5, 5),
	}

	f := NewHTTPFetcher(HTTPOptions{
		UserAgent:    "test",
		RateLimiters: limiters,
	})

	lim := f.limiterFor("https://custom.host.com/path")
	assert.InDelta(t, 5.0, float64(lim.Limit()), 0.001)
}

func TestNewHTTPFetcher_WithRateLimiters(t *testing.T) {
	limiters := map[string]*rate.Limiter{
		"example.com": rate.NewLimiter(1, 1),
	}
	f := NewHTTPFetcher(HTTPOptions{
		RateLimiters: limiters,
	})
	assert.Len(t, f.limiters, 1)
	assert.Contains(t, f.limiters, "example.com")
}

func TestDownload_4xxStatus(t *testing.T) {
	// Test various 4xx status codes - these should NOT be retried
	statuses := []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound}
	for _, code := range statuses {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			f := NewHTTPFetcher(HTTPOptions{
				UserAgent:  "test-agent",
				Timeout:    2 * time.Second,
				MaxRetries: 3,
			})

			_, err := f.Download(context.Background(), srv.URL+"/path")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unexpected status")
			// 4xx should not be retried: only 1 attempt
			assert.Equal(t, int32(1), attempts.Load())
		})
	}
}
