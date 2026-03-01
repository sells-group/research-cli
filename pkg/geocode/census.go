package geocode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

const defaultCensusBaseURL = "https://geocoding.geo.census.gov/geocoder/locations/address"

// CensusProvider geocodes via the Census Bureau's free geocoding API.
type CensusProvider struct {
	client  *http.Client
	baseURL string
}

// CensusOption configures the CensusProvider.
type CensusOption func(*CensusProvider)

// WithCensusHTTPClient sets a custom HTTP client for the Census provider.
func WithCensusHTTPClient(c *http.Client) CensusOption {
	return func(p *CensusProvider) {
		p.client = c
	}
}

// WithCensusBaseURL overrides the Census API base URL (for testing).
func WithCensusBaseURL(u string) CensusOption {
	return func(p *CensusProvider) {
		p.baseURL = u
	}
}

// NewCensusProvider creates a CensusProvider with optional configuration.
func NewCensusProvider(opts ...CensusOption) *CensusProvider {
	p := &CensusProvider{
		client:  http.DefaultClient,
		baseURL: defaultCensusBaseURL,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Name implements Provider.
func (p *CensusProvider) Name() string { return "census" }

// Available implements Provider.
func (p *CensusProvider) Available() bool { return true }

// Geocode implements Provider.
func (p *CensusProvider) Geocode(ctx context.Context, addr AddressInput) (*Result, error) {
	oneLine := formatOneLine(addr)
	if oneLine == "" {
		return &Result{Matched: false, Source: "census"}, nil
	}

	params := url.Values{
		"street":    {addr.Street},
		"city":      {addr.City},
		"state":     {addr.State},
		"zip":       {addr.ZipCode},
		"benchmark": {"Public_AR_Current"},
		"format":    {"json"},
	}

	reqURL := fmt.Sprintf("%s?%s", p.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "census: build request")
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "census: http request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("census: unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "census: read response")
	}

	var cr censusResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		return nil, eris.Wrap(err, "census: parse response")
	}

	if len(cr.Result.AddressMatches) == 0 {
		zap.L().Debug("census geocode: no match", zap.String("address", oneLine))
		return &Result{Matched: false, Source: "census"}, nil
	}

	match := cr.Result.AddressMatches[0]
	result := &Result{
		Latitude:  match.Coordinates.Y,
		Longitude: match.Coordinates.X,
		Source:    "census",
		Quality:   "rooftop",
		Matched:   true,
	}
	// NOTE: Census geocoding API does not return county FIPS directly.
	// State FIPS would require a separate lookup.

	zap.L().Debug("census geocode: match",
		zap.String("address", oneLine),
		zap.String("matched_address", match.MatchedAddress),
		zap.Float64("lat", match.Coordinates.Y),
		zap.Float64("lon", match.Coordinates.X),
	)

	return result, nil
}

// censusResponse represents the Census Bureau geocoder JSON response.
type censusResponse struct {
	Result struct {
		AddressMatches []censusAddressMatch `json:"addressMatches"`
	} `json:"result"`
}

// censusAddressMatch represents a single match from the Census geocoder.
type censusAddressMatch struct {
	Coordinates       censusCoordinates       `json:"coordinates"`
	MatchedAddress    string                  `json:"matchedAddress"`
	AddressComponents censusAddressComponents `json:"addressComponents"`
}

// censusCoordinates holds lon/lat from the Census API (x=lon, y=lat).
type censusCoordinates struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// censusAddressComponents holds parsed address components from the Census API.
type censusAddressComponents struct {
	State string `json:"state"`
	Zip   string `json:"zip"`
}
