package geocode

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoogleGeocode_Rooftop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"status": "OK",
			"results": [{
				"geometry": {
					"location": {"lat": 38.8977, "lng": -77.0365},
					"location_type": "ROOFTOP"
				},
				"formatted_address": "1600 Pennsylvania Avenue NW, Washington, DC 20500"
			}]
		}`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, googleGeocodeURL),
		googleKey:  "test-key",
		limiter:    newTestLimiter(),
	}

	result, err := g.geocodeGoogle(context.Background(), AddressInput{
		Street: "1600 Pennsylvania Ave NW", City: "Washington", State: "DC", ZipCode: "20500",
	})
	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.InDelta(t, 38.8977, result.Latitude, 0.0001)
	assert.InDelta(t, -77.0365, result.Longitude, 0.0001)
	assert.Equal(t, "google", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
}

func TestGoogleGeocode_Approximate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"status": "OK",
			"results": [{
				"geometry": {
					"location": {"lat": 40.7128, "lng": -74.0060},
					"location_type": "APPROXIMATE"
				}
			}]
		}`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, googleGeocodeURL),
		googleKey:  "test-key",
		limiter:    newTestLimiter(),
	}

	result, err := g.geocodeGoogle(context.Background(), AddressInput{
		City: "New York", State: "NY",
	})
	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.Equal(t, "approximate", result.Quality)
}

func TestGoogleGeocode_NoResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status": "ZERO_RESULTS", "results": []}`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, googleGeocodeURL),
		googleKey:  "test-key",
		limiter:    newTestLimiter(),
	}

	result, err := g.geocodeGoogle(context.Background(), AddressInput{
		Street: "000 Nonexistent", City: "Nowhere", State: "XX",
	})
	require.NoError(t, err)
	assert.False(t, result.Matched)
}

func TestGoogleGeocode_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, googleGeocodeURL),
		googleKey:  "test-key",
		limiter:    newTestLimiter(),
	}

	_, err := g.geocodeGoogle(context.Background(), AddressInput{
		Street: "123 Main St", City: "Test", State: "CA",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
}

func TestGoogleGeocode_NoKey(t *testing.T) {
	g := &geocoder{
		httpClient: http.DefaultClient,
		limiter:    newTestLimiter(),
	}

	_, err := g.geocodeGoogle(context.Background(), AddressInput{
		Street: "123 Main St", City: "Test", State: "CA",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

func TestGoogleLocationTypeToQuality(t *testing.T) {
	tests := []struct {
		locType  string
		expected string
	}{
		{"ROOFTOP", "rooftop"},
		{"RANGE_INTERPOLATED", "range"},
		{"GEOMETRIC_CENTER", "centroid"},
		{"APPROXIMATE", "approximate"},
		{"UNKNOWN", "approximate"},
		{"", "approximate"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, googleLocationTypeToQuality(tt.locType), "location_type=%s", tt.locType)
	}
}
