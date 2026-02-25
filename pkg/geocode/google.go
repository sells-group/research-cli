package geocode

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rotisserie/eris"
)

const googleGeocodeURL = "https://maps.googleapis.com/maps/api/geocode/json"

// googleGeocodeResponse is the JSON response from the Google Geocoding API.
type googleGeocodeResponse struct {
	Results []googleResult `json:"results"`
	Status  string         `json:"status"`
}

type googleResult struct {
	Geometry struct {
		Location struct {
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		} `json:"location"`
		LocationType string `json:"location_type"`
	} `json:"geometry"`
	FormattedAddress string `json:"formatted_address"`
}

// geocodeGoogle geocodes a single address using the Google Geocoding API.
func (g *geocoder) geocodeGoogle(ctx context.Context, addr AddressInput) (*Result, error) {
	if g.googleKey == "" {
		return nil, eris.New("geocode: google api key not configured")
	}

	if err := g.limiter.Wait(ctx); err != nil {
		return nil, eris.Wrap(err, "geocode: google rate limit")
	}

	oneLine := formatOneLine(addr)
	params := url.Values{
		"address": {oneLine},
		"key":     {g.googleKey},
	}

	reqURL := googleGeocodeURL + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: google build request")
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: google request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("geocode: google returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: google read body")
	}

	var googleResp googleGeocodeResponse
	if err := json.Unmarshal(body, &googleResp); err != nil {
		return nil, eris.Wrap(err, "geocode: google parse response")
	}

	if googleResp.Status != "OK" || len(googleResp.Results) == 0 {
		return &Result{Matched: false, Source: "google"}, nil
	}

	result := googleResp.Results[0]
	return &Result{
		Latitude:  result.Geometry.Location.Lat,
		Longitude: result.Geometry.Location.Lng,
		Source:    "google",
		Quality:   googleLocationTypeToQuality(result.Geometry.LocationType),
		Matched:   true,
	}, nil
}

// googleLocationTypeToQuality maps Google's location_type to our quality taxonomy.
func googleLocationTypeToQuality(locType string) string {
	switch strings.ToUpper(locType) {
	case "ROOFTOP":
		return "rooftop"
	case "RANGE_INTERPOLATED":
		return "range"
	case "GEOMETRIC_CENTER":
		return "centroid"
	case "APPROXIMATE":
		return "approximate"
	default:
		return "approximate"
	}
}
