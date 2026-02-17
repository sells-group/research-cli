package jina

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRead_Success(t *testing.T) {
	t.Parallel()

	want := ReadResponse{
		Code: 200,
		Data: ReadData{
			Title:   "Acme Corp",
			URL:     "https://acme.com",
			Content: "# Acme Corp\n\nWe build things.",
			Usage:   ReadUsage{Tokens: 2150},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		assert.Equal(t, "markdown", r.Header.Get("X-Return-Format"))
		assert.Equal(t, "/https://acme.com", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	got, err := client.Read(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Data.Title, got.Data.Title)
	assert.Equal(t, want.Data.Content, got.Data.Content)
	assert.Equal(t, want.Data.Usage.Tokens, got.Data.Usage.Tokens)
}

func TestRead_HTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limit exceeded"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
}

func TestRead_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`internal error`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestRead_MalformedJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not json`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestRead_ContextCancellation(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This handler should not be reached because context is cancelled
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(ctx, "https://acme.com")

	require.Error(t, err)
}

func TestRead_EmptyContent(t *testing.T) {
	t.Parallel()

	want := ReadResponse{
		Code: 200,
		Data: ReadData{
			Title:   "",
			URL:     "https://blocked.com",
			Content: "",
			Usage:   ReadUsage{Tokens: 0},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	got, err := client.Read(context.Background(), "https://blocked.com")

	require.NoError(t, err)
	assert.Empty(t, got.Data.Content)
}

func TestWithHTTPClient(t *testing.T) {
	t.Parallel()
	customClient := &http.Client{}
	c := NewClient("test-key", WithHTTPClient(customClient))
	hc := c.(*httpClient)
	assert.Equal(t, customClient, hc.http)
}

func TestNewClient_Defaults(t *testing.T) {
	t.Parallel()
	c := NewClient("my-key")
	hc := c.(*httpClient)
	assert.Equal(t, "my-key", hc.apiKey)
	assert.Equal(t, "https://r.jina.ai", hc.baseURL)
	assert.Equal(t, "https://s.jina.ai", hc.searchBaseURL)
	assert.NotNil(t, hc.http)
	assert.Equal(t, 30*time.Second, hc.http.Timeout)
}

func TestSearch_Success(t *testing.T) {
	t.Parallel()

	want := SearchResponse{
		Code: 200,
		Data: []SearchResult{
			{
				Title:       "Acme Corp BBB Profile",
				URL:         "https://www.bbb.org/us/il/springfield/profile/construction/acme-0001-123",
				Content:     "Acme Corp BBB profile content",
				Description: "BBB accredited business",
			},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		// Should not have X-Return-Format for search.
		assert.Empty(t, r.Header.Get("X-Return-Format"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithSearchBaseURL(srv.URL))
	got, err := client.Search(context.Background(), "Acme Corp BBB")

	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	require.Len(t, got.Data, 1)
	assert.Equal(t, want.Data[0].Title, got.Data[0].Title)
	assert.Equal(t, want.Data[0].URL, got.Data[0].URL)
}

func TestSearch_WithSiteFilter(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.RawQuery, "site=bbb.org")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SearchResponse{Code: 200, Data: []SearchResult{}})
	}))
	defer srv.Close()

	client := NewClient("test-key", WithSearchBaseURL(srv.URL))
	got, err := client.Search(context.Background(), "test query", WithSiteFilter("bbb.org"))

	require.NoError(t, err)
	assert.Equal(t, 200, got.Code)
}

func TestSearch_HTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limit"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithSearchBaseURL(srv.URL))
	_, err := client.Search(context.Background(), "test query")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
}

func TestSearch_MalformedJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not json`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithSearchBaseURL(srv.URL))
	_, err := client.Search(context.Background(), "test query")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestWithSearchBaseURL(t *testing.T) {
	t.Parallel()
	c := NewClient("test-key", WithSearchBaseURL("https://custom.search.ai"))
	hc := c.(*httpClient)
	assert.Equal(t, "https://custom.search.ai", hc.searchBaseURL)
}

func TestRead_RetryOn429(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	want := ReadResponse{
		Code: 200,
		Data: ReadData{Title: "Acme", URL: "https://acme.com", Content: "content"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate limit"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	got, err := client.Read(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, want.Data.Title, got.Data.Title)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRead_RetryExhausted(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`service unavailable`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "503")
	assert.Equal(t, int32(3), attempts.Load()) // 3 attempts total
}

func TestSearch_RetryOn500(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	want := SearchResponse{
		Code: 200,
		Data: []SearchResult{{Title: "Result", URL: "https://example.com"}},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`internal error`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithSearchBaseURL(srv.URL))
	got, err := client.Search(context.Background(), "test query")

	require.NoError(t, err)
	assert.Len(t, got.Data, 1)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestRetryableStatusCode(t *testing.T) {
	assert.True(t, retryableStatusCode(429))
	assert.True(t, retryableStatusCode(500))
	assert.True(t, retryableStatusCode(502))
	assert.True(t, retryableStatusCode(503))
	assert.False(t, retryableStatusCode(200))
	assert.False(t, retryableStatusCode(404))
	assert.False(t, retryableStatusCode(422))
}
