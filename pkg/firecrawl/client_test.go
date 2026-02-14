package firecrawl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, Client) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := NewClient("test-api-key", WithBaseURL(srv.URL))
	return srv, c
}

func TestCrawl(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantID     string
		wantErr    bool
		wantAPIErr bool
		wantStatus int
	}{
		{
			name: "happy path",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/crawl", r.URL.Path)
				assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

				var req CrawlRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				assert.Equal(t, "https://example.com", req.URL)

				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(CrawlResponse{Success: true, ID: "crawl-123"})
			},
			wantID: "crawl-123",
		},
		{
			name: "auth error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"Unauthorized"}`))
			},
			wantErr:    true,
			wantAPIErr: true,
			wantStatus: 401,
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error":"internal server error"}`))
			},
			wantErr:    true,
			wantAPIErr: true,
			wantStatus: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, c := newTestServer(t, tt.handler)
			resp, err := c.Crawl(context.Background(), CrawlRequest{URL: "https://example.com"})

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantAPIErr {
					var apiErr *APIError
					require.ErrorAs(t, err, &apiErr)
					assert.Equal(t, tt.wantStatus, apiErr.StatusCode)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantID, resp.ID)
			assert.True(t, resp.Success)
		})
	}
}

func TestGetCrawlStatus(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus string
		wantPages  int
		wantErr    bool
	}{
		{
			name: "completed",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/crawl/crawl-123", r.URL.Path)
				assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

				json.NewEncoder(w).Encode(CrawlStatusResponse{
					Status: "completed",
					Total:  2,
					Data: []PageData{
						{URL: "https://example.com", Markdown: "# Home", Title: "Home", StatusCode: 200},
						{URL: "https://example.com/about", Markdown: "# About", Title: "About", StatusCode: 200},
					},
				})
			},
			wantStatus: "completed",
			wantPages:  2,
		},
		{
			name: "still scraping",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(CrawlStatusResponse{
					Status: "scraping",
					Total:  5,
				})
			},
			wantStatus: "scraping",
			wantPages:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, c := newTestServer(t, tt.handler)
			resp, err := c.GetCrawlStatus(context.Background(), "crawl-123")

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.Status)
			assert.Len(t, resp.Data, tt.wantPages)
		})
	}
}

func TestScrape(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantTitle  string
		wantErr    bool
		wantAPIErr bool
		wantStatus int
	}{
		{
			name: "happy path",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/scrape", r.URL.Path)

				var req ScrapeRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				assert.Equal(t, "https://example.com/about", req.URL)

				json.NewEncoder(w).Encode(ScrapeResponse{
					Success: true,
					Data: PageData{
						URL:        "https://example.com/about",
						Markdown:   "# About Us",
						Title:      "About",
						StatusCode: 200,
					},
				})
			},
			wantTitle: "About",
		},
		{
			name: "rate limited",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limited"}`))
			},
			wantErr:    true,
			wantAPIErr: true,
			wantStatus: 429,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, c := newTestServer(t, tt.handler)
			resp, err := c.Scrape(context.Background(), ScrapeRequest{URL: "https://example.com/about"})

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantAPIErr {
					var apiErr *APIError
					require.ErrorAs(t, err, &apiErr)
					assert.Equal(t, tt.wantStatus, apiErr.StatusCode)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTitle, resp.Data.Title)
			assert.True(t, resp.Success)
		})
	}
}

func TestBatchScrape(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/batch/scrape", r.URL.Path)

		var req BatchScrapeRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Len(t, req.URLs, 3)

		json.NewEncoder(w).Encode(BatchScrapeResponse{
			Success: true,
			ID:      "batch-456",
		})
	})

	resp, err := c.BatchScrape(context.Background(), BatchScrapeRequest{
		URLs: []string{"https://a.com", "https://b.com", "https://c.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "batch-456", resp.ID)
	assert.True(t, resp.Success)
}

func TestGetBatchScrapeStatus(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/batch/scrape/batch-456", r.URL.Path)

		json.NewEncoder(w).Encode(BatchScrapeStatusResponse{
			Status: "completed",
			Total:  2,
			Data: []PageData{
				{URL: "https://a.com", Markdown: "# A", Title: "A", StatusCode: 200},
				{URL: "https://b.com", Markdown: "# B", Title: "B", StatusCode: 200},
			},
		})
	})

	resp, err := c.GetBatchScrapeStatus(context.Background(), "batch-456")
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Len(t, resp.Data, 2)
}

func TestContextCancellation(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Should never reach here
		t.Fatal("request should have been cancelled")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := c.Crawl(ctx, CrawlRequest{URL: "https://example.com"})
	require.Error(t, err)
}

func TestAPIError_Error(t *testing.T) {
	t.Parallel()
	e := &APIError{StatusCode: 429, Body: `{"error":"rate limited"}`}
	assert.Equal(t, `firecrawl: HTTP 429: {"error":"rate limited"}`, e.Error())
}

func TestWithHTTPClient(t *testing.T) {
	t.Parallel()
	customClient := &http.Client{}
	c := NewClient("key", WithHTTPClient(customClient))
	hc := c.(*httpClient)
	assert.Equal(t, customClient, hc.http)
}

func TestMalformedJSON(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{not json`))
	})

	_, err := c.Scrape(context.Background(), ScrapeRequest{URL: "https://example.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestGetCrawlStatus_Error(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"not found"}`))
	})

	_, err := c.GetCrawlStatus(context.Background(), "nonexistent")
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 404, apiErr.StatusCode)
}

func TestGetBatchScrapeStatus_Error(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"not found"}`))
	})

	_, err := c.GetBatchScrapeStatus(context.Background(), "nonexistent")
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 404, apiErr.StatusCode)
}

func TestBatchScrape_Error(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limited"}`))
	})

	_, err := c.BatchScrape(context.Background(), BatchScrapeRequest{
		URLs: []string{"https://a.com"},
	})
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 429, apiErr.StatusCode)
}
