// Package overpass provides a client for the OpenStreetMap Overpass API.
package overpass

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rotisserie/eris"
)

// DefaultEndpoint is the public Overpass API endpoint.
const DefaultEndpoint = "https://overpass-api.de/api/interpreter"

// Element represents a single OSM element from the Overpass API.
type Element struct {
	Type string            `json:"type"`
	ID   int64             `json:"id"`
	Lat  float64           `json:"lat"`
	Lon  float64           `json:"lon"`
	Tags map[string]string `json:"tags"`
}

// Response is the Overpass API response envelope.
type Response struct {
	Elements []Element `json:"elements"`
}

// Query sends an Overpass QL query and returns parsed elements.
// It uses a standard HTTP POST to the Overpass interpreter endpoint.
func Query(ctx context.Context, endpoint, query string) ([]Element, error) {
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}

	form := url.Values{"data": {query}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, eris.Wrap(err, "overpass: build request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "overpass: execute request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, eris.Errorf("overpass: HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, eris.Wrap(err, "overpass: decode response")
	}

	return result.Elements, nil
}

// BuildPOIQuery constructs an Overpass QL query for common US POI types
// within the given bounding box (south, west, north, east).
func BuildPOIQuery(south, west, north, east float64) string {
	bbox := fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", south, west, north, east)
	return fmt.Sprintf(`[out:json][timeout:120];
(
  node["amenity"~"school|hospital|fire_station|police|library|post_office|place_of_worship"](%s);
  node["leisure"="park"](%s);
);
out body;`, bbox, bbox)
}
