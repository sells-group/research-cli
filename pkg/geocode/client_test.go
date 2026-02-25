package geocode

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeClient_CensusSucceeds_NoGoogleCall(t *testing.T) {
	var googleCalled atomic.Int32

	censusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"result": {
				"addressMatches": [{
					"coordinates": {"x": -77.0365, "y": 38.8977},
					"matchedAddress": "1600 Pennsylvania Ave NW"
				}]
			}
		}`)
	}))
	defer censusSrv.Close()

	googleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		googleCalled.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"OK","results":[{"geometry":{"location":{"lat":38.9,"lng":-77.0},"location_type":"ROOFTOP"}}]}`)
	}))
	defer googleSrv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(censusSrv.URL, censusOneLineURL),
		googleKey:  "test-key",
		limiter:    newTestLimiter(),
	}

	result, err := g.Geocode(context.Background(), AddressInput{
		Street: "1600 Pennsylvania Ave NW", City: "Washington", State: "DC",
	})
	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
	assert.Equal(t, int32(0), googleCalled.Load(), "Google should not be called when Census succeeds")
}

func TestCompositeClient_CensusFails_GoogleFallback(t *testing.T) {
	// Census returns no match.
	censusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"result": {"addressMatches": []}}`)
	}))
	defer censusSrv.Close()

	// Google returns a match.
	googleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"status": "OK",
			"results": [{
				"geometry": {
					"location": {"lat": 40.7128, "lng": -74.0060},
					"location_type": "ROOFTOP"
				}
			}]
		}`)
	}))
	defer googleSrv.Close()

	// Build a client that routes Census to censusSrv and Google to googleSrv.
	g := &geocoder{
		httpClient: &http.Client{
			Transport: &multiRewriteTransport{
				base: http.DefaultTransport,
				rewrites: map[string]string{
					censusOneLineURL: censusSrv.URL,
					googleGeocodeURL: googleSrv.URL,
				},
			},
		},
		googleKey: "test-key",
		limiter:   newTestLimiter(),
	}

	result, err := g.Geocode(context.Background(), AddressInput{
		Street: "123 Main St", City: "New York", State: "NY",
	})
	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.Equal(t, "google", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
}

func TestCompositeClient_BothFail_NoMatch(t *testing.T) {
	// Census returns no match.
	censusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"result": {"addressMatches": []}}`)
	}))
	defer censusSrv.Close()

	// Google returns no results.
	googleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status": "ZERO_RESULTS", "results": []}`)
	}))
	defer googleSrv.Close()

	g := &geocoder{
		httpClient: &http.Client{
			Transport: &multiRewriteTransport{
				base: http.DefaultTransport,
				rewrites: map[string]string{
					censusOneLineURL: censusSrv.URL,
					googleGeocodeURL: googleSrv.URL,
				},
			},
		},
		googleKey: "test-key",
		limiter:   newTestLimiter(),
	}

	result, err := g.Geocode(context.Background(), AddressInput{
		Street: "000 Nowhere", City: "Faketown", State: "XX",
	})
	require.NoError(t, err)
	assert.False(t, result.Matched)
}

func TestCompositeClient_NoGoogleKey_CensusOnly(t *testing.T) {
	// Census returns no match.
	censusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"result": {"addressMatches": []}}`)
	}))
	defer censusSrv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(censusSrv.URL, censusOneLineURL),
		limiter:    newTestLimiter(),
		// No googleKey set.
	}

	result, err := g.Geocode(context.Background(), AddressInput{
		Street: "123 Main St", City: "Test", State: "CA",
	})
	require.NoError(t, err)
	assert.False(t, result.Matched)
}

// multiRewriteTransport rewrites URLs based on a prefix map.
type multiRewriteTransport struct {
	base     http.RoundTripper
	rewrites map[string]string
}

func (t *multiRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	origURL := req.URL.String()
	for prefix, testURL := range t.rewrites {
		if len(origURL) >= len(prefix) && origURL[:len(prefix)] == prefix {
			suffix := origURL[len(prefix):]
			newURL := testURL + suffix
			newReq := req.Clone(req.Context())
			parsed, err := req.URL.Parse(newURL)
			if err != nil {
				return nil, err
			}
			newReq.URL = parsed
			newReq.Host = parsed.Host
			return t.base.RoundTrip(newReq)
		}
	}
	return t.base.RoundTrip(req)
}
