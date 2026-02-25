// Package google provides a client for Google APIs used in enrichment.
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
	DiscoverySearch(ctx context.Context, req DiscoverySearchRequest) (*DiscoverySearchResponse, error)
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

// DiscoverySearchRequest is the request for a discovery-oriented Places text search.
type DiscoverySearchRequest struct {
	TextQuery           string        `json:"textQuery"`
	LocationRestriction *LocationRect `json:"locationRestriction,omitempty"`
	PageToken           string        `json:"pageToken,omitempty"`
}

// LocationRect restricts search results to a geographic rectangle.
type LocationRect struct {
	Rectangle Rectangle `json:"rectangle"`
}

// Rectangle defines a geographic bounding box with low (SW) and high (NE) corners.
type Rectangle struct {
	Low  LatLng `json:"low"`
	High LatLng `json:"high"`
}

// LatLng represents a geographic coordinate.
type LatLng struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// DiscoverySearchResponse is the response from a discovery search.
type DiscoverySearchResponse struct {
	Places        []DiscoveryPlace `json:"places"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

// DiscoveryPlace is a place returned by discovery search with fields relevant to lead qualification.
type DiscoveryPlace struct {
	ID               string      `json:"id"`
	DisplayName      DisplayName `json:"displayName"`
	WebsiteURI       string      `json:"websiteUri"`
	FormattedAddress string      `json:"formattedAddress"`
	Location         *LatLng     `json:"location,omitempty"`
}

const discoveryFieldMask = "places.id,places.displayName.text,places.websiteUri,places.formattedAddress,places.location"

// DiscoverySearch implements Client.
func (c *httpClient) DiscoverySearch(ctx context.Context, req DiscoverySearchRequest) (*DiscoverySearchResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, eris.Wrap(err, "google: marshal discovery request")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/places:searchText", bytes.NewReader(body))
	if err != nil {
		return nil, eris.Wrap(err, "google: create discovery request")
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Goog-Api-Key", c.apiKey)
	httpReq.Header.Set("X-Goog-FieldMask", discoveryFieldMask)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, eris.Wrap(err, "google: send discovery request")
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "google: read discovery response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("google: discovery search status %d: %s", resp.StatusCode, string(respBody))
	}

	var result DiscoverySearchResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, eris.Wrap(err, "google: unmarshal discovery response")
	}

	return &result, nil
}
