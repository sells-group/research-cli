package jina

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rotisserie/eris"
)

// Client defines the Jina AI Reader operations.
type Client interface {
	// Read fetches a URL via Jina AI Reader and returns the markdown content.
	Read(ctx context.Context, targetURL string) (*ReadResponse, error)
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

// Option configures the Jina client.
type Option func(*httpClient)

// WithBaseURL sets a custom base URL (for testing).
func WithBaseURL(url string) Option {
	return func(c *httpClient) {
		c.baseURL = url
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *httpClient) {
		c.http = hc
	}
}

type httpClient struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// NewClient creates a new Jina AI Reader client.
func NewClient(apiKey string, opts ...Option) Client {
	c := &httpClient{
		apiKey:  apiKey,
		baseURL: "https://r.jina.ai",
		http:    http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
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

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "jina: request failed")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "jina: read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("jina: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result ReadResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, eris.Wrap(err, "jina: unmarshal response")
	}

	return &result, nil
}
