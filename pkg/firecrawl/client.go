package firecrawl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
)

// Default base URL for the Firecrawl v2 API.
const defaultBaseURL = "https://api.firecrawl.dev/v1"

// Client defines the Firecrawl v2 API operations.
type Client interface {
	Crawl(ctx context.Context, req CrawlRequest) (*CrawlResponse, error)
	GetCrawlStatus(ctx context.Context, id string) (*CrawlStatusResponse, error)
	Scrape(ctx context.Context, req ScrapeRequest) (*ScrapeResponse, error)
	BatchScrape(ctx context.Context, req BatchScrapeRequest) (*BatchScrapeResponse, error)
	GetBatchScrapeStatus(ctx context.Context, id string) (*BatchScrapeStatusResponse, error)
}

// CrawlRequest is the body for POST /crawl.
type CrawlRequest struct {
	URL      string `json:"url"`
	MaxDepth int    `json:"maxDepth,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// CrawlResponse is the response from POST /crawl.
type CrawlResponse struct {
	Success bool   `json:"success"`
	ID      string `json:"id"`
}

// CrawlStatusResponse is the response from GET /crawl/{id}.
type CrawlStatusResponse struct {
	Status string     `json:"status"`
	Total  int        `json:"total"`
	Data   []PageData `json:"data"`
}

// ScrapeRequest is the body for POST /scrape.
type ScrapeRequest struct {
	URL     string   `json:"url"`
	Formats []string `json:"formats,omitempty"`
}

// ScrapeResponse is the response from POST /scrape.
type ScrapeResponse struct {
	Success bool     `json:"success"`
	Data    PageData `json:"data"`
}

// BatchScrapeRequest is the body for POST /batch/scrape.
type BatchScrapeRequest struct {
	URLs    []string `json:"urls"`
	Formats []string `json:"formats,omitempty"`
}

// BatchScrapeResponse is the response from POST /batch/scrape.
type BatchScrapeResponse struct {
	Success bool   `json:"success"`
	ID      string `json:"id"`
}

// BatchScrapeStatusResponse is the response from GET /batch/scrape/{id}.
type BatchScrapeStatusResponse struct {
	Status string     `json:"status"`
	Total  int        `json:"total"`
	Data   []PageData `json:"data"`
}

// PageData represents a single page result from Firecrawl.
type PageData struct {
	URL        string `json:"url"`
	Markdown   string `json:"markdown"`
	Title      string `json:"title"`
	StatusCode int    `json:"statusCode"`
}

// APIError is returned when Firecrawl responds with a non-2xx status.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("firecrawl: HTTP %d: %s", e.StatusCode, e.Body)
}

// Option configures the httpClient.
type Option func(*httpClient)

// WithBaseURL overrides the default base URL.
func WithBaseURL(url string) Option {
	return func(c *httpClient) {
		c.baseURL = url
	}
}

// WithHTTPClient sets a custom *http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *httpClient) {
		c.http = hc
	}
}

// httpClient implements Client using net/http.
type httpClient struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// NewClient creates a new Firecrawl client.
func NewClient(apiKey string, opts ...Option) Client {
	c := &httpClient{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		http: &http.Client{
			Timeout: 60 * time.Second,
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

func (c *httpClient) Crawl(ctx context.Context, req CrawlRequest) (*CrawlResponse, error) {
	var resp CrawlResponse
	if err := c.post(ctx, "/crawl", req, &resp); err != nil {
		return nil, eris.Wrap(err, "firecrawl: start crawl")
	}
	return &resp, nil
}

func (c *httpClient) GetCrawlStatus(ctx context.Context, id string) (*CrawlStatusResponse, error) {
	var resp CrawlStatusResponse
	if err := c.get(ctx, fmt.Sprintf("/crawl/%s", id), &resp); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("firecrawl: get crawl status %s", id))
	}
	return &resp, nil
}

func (c *httpClient) Scrape(ctx context.Context, req ScrapeRequest) (*ScrapeResponse, error) {
	var resp ScrapeResponse
	if err := c.post(ctx, "/scrape", req, &resp); err != nil {
		return nil, eris.Wrap(err, "firecrawl: scrape")
	}
	return &resp, nil
}

func (c *httpClient) BatchScrape(ctx context.Context, req BatchScrapeRequest) (*BatchScrapeResponse, error) {
	var resp BatchScrapeResponse
	if err := c.post(ctx, "/batch/scrape", req, &resp); err != nil {
		return nil, eris.Wrap(err, "firecrawl: start batch scrape")
	}
	return &resp, nil
}

func (c *httpClient) GetBatchScrapeStatus(ctx context.Context, id string) (*BatchScrapeStatusResponse, error) {
	var resp BatchScrapeStatusResponse
	if err := c.get(ctx, fmt.Sprintf("/batch/scrape/%s", id), &resp); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("firecrawl: get batch scrape status %s", id))
	}
	return &resp, nil
}

func (c *httpClient) post(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return eris.Wrap(err, "marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return eris.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	return c.do(req, out)
}

func (c *httpClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return eris.Wrap(err, "create request")
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	return c.do(req, out)
}

func (c *httpClient) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return eris.Wrap(err, "execute request")
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return eris.Wrap(err, "read response body")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(data),
		}
	}

	if err := json.Unmarshal(data, out); err != nil {
		return eris.Wrap(err, "decode response")
	}

	return nil
}
