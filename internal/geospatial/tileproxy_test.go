package geospatial

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTileProxy_Fetch_Success(t *testing.T) {
	tileData := []byte("fake-png-tile-data")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(tileData)
	}))
	defer upstream.Close()

	proxy := NewTileProxy(upstream.URL, "png", nil)
	data, ct, err := proxy.Fetch(context.Background(), 10, 512, 384)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(tileData) {
		t.Errorf("expected tile data %q, got %q", tileData, data)
	}
	if ct != "image/png" {
		t.Errorf("expected content type image/png, got %s", ct)
	}
}

func TestTileProxy_Fetch_CacheHit(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte("tile"))
	}))
	defer upstream.Close()

	cache := NewTileCache(100, 10*time.Minute)
	proxy := NewTileProxy(upstream.URL, "png", cache)

	// First fetch — cache miss.
	_, _, err := proxy.Fetch(context.Background(), 5, 10, 10)
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 upstream call, got %d", calls)
	}

	// Second fetch — cache hit, no upstream call.
	_, _, err = proxy.Fetch(context.Background(), 5, 10, 10)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 upstream call (cached), got %d", calls)
	}
}

func TestTileProxy_Fetch_UpstreamError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	proxy := NewTileProxy(upstream.URL, "png", nil)
	_, _, err := proxy.Fetch(context.Background(), 1, 0, 0)
	if err == nil {
		t.Fatal("expected error for 500 upstream response")
	}
}

func TestTileProxy_ContentType(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{"png", "image/png"},
		{"jpg", "image/jpeg"},
		{"jpeg", "image/jpeg"},
		{"webp", "image/webp"},
		{"unknown", "application/octet-stream"},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			proxy := NewTileProxy("http://example.com", tt.format, nil)
			if got := proxy.contentType(); got != tt.want {
				t.Errorf("contentType(%s) = %s, want %s", tt.format, got, tt.want)
			}
		})
	}
}

func TestTileProxy_ServeHTTP_Success(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tile-bytes"))
	}))
	defer upstream.Close()

	proxy := NewTileProxy(upstream.URL, "png", nil)

	req := httptest.NewRequest(http.MethodGet, "/10/512/384.png", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "image/png" {
		t.Errorf("expected image/png, got %s", w.Header().Get("Content-Type"))
	}
}

func TestTileProxy_ServeHTTP_InvalidPath(t *testing.T) {
	proxy := NewTileProxy("http://example.com", "png", nil)

	req := httptest.NewRequest(http.MethodGet, "/bad/path", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestTileProxy_ServeHTTP_FetchError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	proxy := NewTileProxy(upstream.URL, "png", nil)

	req := httptest.NewRequest(http.MethodGet, "/10/512/384.png", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

func TestTileProxy_Fetch_ConnectionError(t *testing.T) {
	proxy := NewTileProxy("http://127.0.0.1:1", "png", nil)
	_, _, err := proxy.Fetch(context.Background(), 1, 0, 0)
	if err == nil {
		t.Fatal("expected error for unreachable host")
	}
}
