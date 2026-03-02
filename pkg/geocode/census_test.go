package geocode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCensusProvider_Match(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"result": {
				"addressMatches": [
					{
						"coordinates": {"x": -77.016, "y": 38.899},
						"matchedAddress": "1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500",
						"addressComponents": {
							"state": "DC",
							"zip": "20500"
						}
					}
				]
			}
		}`))
	}))
	defer srv.Close()

	p := NewCensusProvider(WithCensusBaseURL(srv.URL))
	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "1600 Pennsylvania Ave NW",
		City:    "Washington",
		State:   "DC",
		ZipCode: "20500",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
	assert.InDelta(t, 38.899, result.Latitude, 0.001)
	assert.InDelta(t, -77.016, result.Longitude, 0.001)
}

func TestCensusProvider_NoMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"result": {
				"addressMatches": []
			}
		}`))
	}))
	defer srv.Close()

	p := NewCensusProvider(WithCensusBaseURL(srv.URL))
	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "999 Nonexistent Ave",
		City:    "Nowhere",
		State:   "XX",
		ZipCode: "00000",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
	assert.Equal(t, "census", result.Source)
}

func TestCensusProvider_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewCensusProvider(WithCensusBaseURL(srv.URL))
	_, err := p.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status 500")
}

func TestCensusProvider_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid json`))
	}))
	defer srv.Close()

	p := NewCensusProvider(WithCensusBaseURL(srv.URL))
	_, err := p.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse response")
}

func TestCensusProvider_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"addressMatches":[]}}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before request

	p := NewCensusProvider(WithCensusBaseURL(srv.URL))
	_, err := p.Geocode(ctx, AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	assert.Error(t, err)
}

func TestCensusProvider_EmptyAddress(t *testing.T) {
	p := NewCensusProvider()
	result, err := p.Geocode(context.Background(), AddressInput{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
	assert.Equal(t, "census", result.Source)
}

func TestCensusProvider_Name(t *testing.T) {
	p := NewCensusProvider()
	assert.Equal(t, "census", p.Name())
}

func TestCensusProvider_Available(t *testing.T) {
	p := NewCensusProvider()
	assert.True(t, p.Available())
}

func TestCensusProvider_WithHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"result": {
				"addressMatches": [
					{
						"coordinates": {"x": -80.19, "y": 25.77},
						"matchedAddress": "100 MAIN ST, MIAMI, FL, 33131",
						"addressComponents": {
							"state": "FL",
							"zip": "33131"
						}
					}
				]
			}
		}`))
	}))
	defer srv.Close()

	customClient := &http.Client{}
	p := NewCensusProvider(
		WithCensusHTTPClient(customClient),
		WithCensusBaseURL(srv.URL),
	)

	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "100 Main St",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)
	assert.InDelta(t, -80.19, result.Longitude, 0.01)
}
