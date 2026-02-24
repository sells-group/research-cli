package google

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
)

const defaultBaseURL = "https://places.googleapis.com/v1"

// Client performs Google Places API operations.
type Client interface {
	TextSearch(ctx context.Context, query string) (*TextSearchResponse, error)
}

// TextSearchResponse is the response from Places Text Search.
type TextSearchResponse struct {
	Places []Place `json:"places"`
}

// Place represents a place returned by the API.
type Place struct {
	DisplayName     DisplayName `json:"displayName"`
	Rating          float64     `json:"rating"`
	UserRatingCount int         `json:"userRatingCount"`
}

// DisplayName holds the place's display name.
type DisplayName struct {
	Text string `json:"text"`
}

// Option configures the client.
type Option func(*httpClient)

// WithBaseURL overrides the default API base URL.
func WithBaseURL(url string) Option {
	return func(c *httpClient) {
		c.baseURL = url
	}
}

// WithHTTPClient overrides the default http.Client.
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

// NewClient creates a Google Places API client.
func NewClient(apiKey string, opts ...Option) Client {
	c := &httpClient{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

type textSearchRequest struct {
	TextQuery string `json:"textQuery"`
}

func (c *httpClient) TextSearch(ctx context.Context, query string) (*TextSearchResponse, error) {
	body, err := json.Marshal(textSearchRequest{TextQuery: query})
	if err != nil {
		return nil, eris.Wrap(err, "google: marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/places:searchText", bytes.NewReader(body))
	if err != nil {
		return nil, eris.Wrap(err, "google: create request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Goog-Api-Key", c.apiKey)
	req.Header.Set("X-Goog-FieldMask", "places.rating,places.userRatingCount,places.displayName")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "google: send request")
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "google: read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("google: unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var result TextSearchResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, eris.Wrap(err, "google: unmarshal response")
	}

	return &result, nil
}
