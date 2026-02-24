// Package jina provides a client for the Jina AI reader and search API.
package jina

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/rotisserie/eris"
)

// Client defines the Jina AI Reader operations.
type Client interface {
	// Read fetches a URL via Jina AI Reader and returns the markdown content.
	Read(ctx context.Context, targetURL string) (*ReadResponse, error)
	// Search performs a web search via Jina AI Search and returns results.
	Search(ctx context.Context, query string, opts ...SearchOption) (*SearchResponse, error)
}

// ReadResponse is the parsed Jina API response.
type ReadResponse struct {
	Code int      `json:"code"`
	Data ReadData `json:"data"`
}

// ReadData holds the content from Jina.
type ReadData struct {
	Title   string    `json:"title"`
	URL     string    `json:"url"`
	Content string    `json:"content"`
	Usage   ReadUsage `json:"usage"`
}

// ReadUsage tracks token consumption.
type ReadUsage struct {
	Tokens int `json:"tokens"`
}

// SearchResponse is the parsed Jina Search API response.
type SearchResponse struct {
	Code int            `json:"code"`
	Data []SearchResult `json:"data"`
}

// SearchResult represents a single search result.
type SearchResult struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Content     string `json:"content"`
	Description string `json:"description"`
}

// SearchOption configures a search request.
type SearchOption func(*searchOpts)

type searchOpts struct {
	siteFilter string
}

// WithSiteFilter restricts search results to a specific domain.
func WithSiteFilter(domain string) SearchOption {
	return func(o *searchOpts) {
		o.siteFilter = domain
	}
}

// Option configures the Jina client.
type Option func(*httpClient)

// WithBaseURL sets a custom base URL (for testing).
func WithBaseURL(url string) Option {
	return func(c *httpClient) {
		c.baseURL = url
	}
}

// WithSearchBaseURL sets a custom search base URL (for testing).
func WithSearchBaseURL(url string) Option {
	return func(c *httpClient) {
		c.searchBaseURL = url
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *httpClient) {
		c.http = hc
	}
}

type httpClient struct {
	apiKey        string
	baseURL       string
	searchBaseURL string
	http          *http.Client
}

// NewClient creates a new Jina AI Reader client.
func NewClient(apiKey string, opts ...Option) Client {
	c := &httpClient{
		apiKey:        apiKey,
		baseURL:       "https://r.jina.ai",
		searchBaseURL: "https://s.jina.ai",
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// retryableStatusCode returns true if the HTTP status code should trigger a retry.
func retryableStatusCode(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusInternalServerError ||
		code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable
}

// retryDo executes an HTTP request with exponential backoff retries on
// transient failures (429, 500, 502, 503). Returns the response body and
// status code on success, or the last error after exhausting retries.
func (c *httpClient) retryDo(ctx context.Context, req *http.Request) ([]byte, int, error) {
	const maxAttempts = 3
	backoff := 1 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Clone request for retry (body is nil for GET requests).
		retryReq := req.Clone(ctx)

		resp, err := c.http.Do(retryReq)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				case <-time.After(backoff):
				}
				backoff *= 2
				continue
			}
			return nil, 0, lastErr
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, resp.StatusCode, eris.Wrap(readErr, "jina: read response body")
		}

		if retryableStatusCode(resp.StatusCode) && attempt < maxAttempts {
			lastErr = eris.Errorf("jina: status %d: %s", resp.StatusCode, string(body))
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}

		return body, resp.StatusCode, nil
	}

	return nil, 0, lastErr
}

func (c *httpClient) Read(ctx context.Context, targetURL string) (*ReadResponse, error) {
	reqURL := fmt.Sprintf("%s/%s", c.baseURL, targetURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "jina: create request")
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Return-Format", "markdown")

	body, statusCode, err := c.retryDo(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "jina: request failed")
	}

	if statusCode != http.StatusOK {
		return nil, eris.Errorf("jina: unexpected status %d: %s", statusCode, string(body))
	}

	var result ReadResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, eris.Wrap(err, "jina: unmarshal response")
	}

	return &result, nil
}

func (c *httpClient) Search(ctx context.Context, query string, opts ...SearchOption) (*SearchResponse, error) {
	so := &searchOpts{}
	for _, opt := range opts {
		opt(so)
	}

	reqURL := fmt.Sprintf("%s/%s", c.searchBaseURL, url.QueryEscape(query))

	if so.siteFilter != "" {
		reqURL += "?site=" + url.QueryEscape(so.siteFilter)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "jina: create search request")
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	body, statusCode, err := c.retryDo(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "jina: search request failed")
	}

	// Jina returns 422 when no results are available for the query.
	// Treat this as empty results rather than an error.
	if statusCode == http.StatusUnprocessableEntity {
		return &SearchResponse{Code: 422}, nil
	}

	if statusCode != http.StatusOK {
		return nil, eris.Errorf("jina: search unexpected status %d: %s", statusCode, string(body))
	}

	var result SearchResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, eris.Wrap(err, "jina: unmarshal search response")
	}

	return &result, nil
}
