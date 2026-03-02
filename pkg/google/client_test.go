package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextSearch_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/places:searchText", r.URL.Path)
		assert.Equal(t, "test-key", r.Header.Get("X-Goog-Api-Key"))
		assert.Contains(t, r.Header.Get("X-Goog-FieldMask"), "places.rating")

		var body textSearchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "Acme Corp Springfield IL", body.TextQuery)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TextSearchResponse{
			Places: []Place{
				{
					DisplayName:     DisplayName{Text: "Acme Corp"},
					Rating:          4.5,
					UserRatingCount: 127,
				},
			},
		})
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.TextSearch(context.Background(), "Acme Corp Springfield IL")

	require.NoError(t, err)
	require.Len(t, resp.Places, 1)
	assert.Equal(t, "Acme Corp", resp.Places[0].DisplayName.Text)
	assert.InDelta(t, 4.5, resp.Places[0].Rating, 0.001)
	assert.Equal(t, 127, resp.Places[0].UserRatingCount)
}

func TestTextSearch_NoResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TextSearchResponse{Places: nil})
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.TextSearch(context.Background(), "Nonexistent Corp")

	require.NoError(t, err)
	assert.Empty(t, resp.Places)
}

func TestTextSearch_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error": "invalid API key"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	client := NewClient("bad-key", WithBaseURL(srv.URL))
	resp, err := client.TextSearch(context.Background(), "test query")

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "403")
}

func TestTextSearch_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Simulate slow response — context should cancel first.
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.TextSearch(ctx, "test")

	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestDiscoverySearch_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/places:searchText", r.URL.Path)
		assert.Equal(t, "test-key", r.Header.Get("X-Goog-Api-Key"))
		assert.Contains(t, r.Header.Get("X-Goog-FieldMask"), "places.id")
		assert.Contains(t, r.Header.Get("X-Goog-FieldMask"), "places.websiteUri")

		var body DiscoverySearchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "accounting firms", body.TextQuery)
		require.NotNil(t, body.LocationRestriction)
		assert.InDelta(t, 30.0, body.LocationRestriction.Rectangle.Low.Latitude, 0.001)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(DiscoverySearchResponse{
			Places: []DiscoveryPlace{
				{
					ID:               "ChIJ-test1",
					DisplayName:      DisplayName{Text: "Smith & Co CPAs"},
					WebsiteURI:       "https://smithcpas.com",
					FormattedAddress: "123 Main St, Springfield, IL 62701",
					Location:         &LatLng{Latitude: 30.5, Longitude: -90.5},
				},
			},
			NextPageToken: "next-page-token-123",
		})
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.DiscoverySearch(context.Background(), DiscoverySearchRequest{
		TextQuery: "accounting firms",
		LocationRestriction: &LocationRect{
			Rectangle: Rectangle{
				Low:  LatLng{Latitude: 30.0, Longitude: -91.0},
				High: LatLng{Latitude: 31.0, Longitude: -90.0},
			},
		},
	})

	require.NoError(t, err)
	require.Len(t, resp.Places, 1)
	assert.Equal(t, "ChIJ-test1", resp.Places[0].ID)
	assert.Equal(t, "Smith & Co CPAs", resp.Places[0].DisplayName.Text)
	assert.Equal(t, "https://smithcpas.com", resp.Places[0].WebsiteURI)
	assert.Equal(t, "next-page-token-123", resp.NextPageToken)
}

func TestDiscoverySearch_Pagination(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var body DiscoverySearchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))

		w.Header().Set("Content-Type", "application/json")
		if body.PageToken == "" {
			_ = json.NewEncoder(w).Encode(DiscoverySearchResponse{
				Places:        []DiscoveryPlace{{ID: "place-1", DisplayName: DisplayName{Text: "First"}}},
				NextPageToken: "page-2-token",
			})
		} else {
			assert.Equal(t, "page-2-token", body.PageToken)
			_ = json.NewEncoder(w).Encode(DiscoverySearchResponse{
				Places: []DiscoveryPlace{{ID: "place-2", DisplayName: DisplayName{Text: "Second"}}},
			})
		}
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))

	// First page.
	resp, err := client.DiscoverySearch(context.Background(), DiscoverySearchRequest{TextQuery: "test"})
	require.NoError(t, err)
	require.Len(t, resp.Places, 1)
	assert.Equal(t, "place-1", resp.Places[0].ID)
	assert.Equal(t, "page-2-token", resp.NextPageToken)

	// Second page.
	resp, err = client.DiscoverySearch(context.Background(), DiscoverySearchRequest{
		TextQuery: "test",
		PageToken: resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, resp.Places, 1)
	assert.Equal(t, "place-2", resp.Places[0].ID)
	assert.Empty(t, resp.NextPageToken)

	assert.Equal(t, 2, callCount)
}

func TestDiscoverySearch_NoResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(DiscoverySearchResponse{})
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.DiscoverySearch(context.Background(), DiscoverySearchRequest{TextQuery: "nonexistent"})

	require.NoError(t, err)
	assert.Empty(t, resp.Places)
	assert.Empty(t, resp.NextPageToken)
}

func TestDiscoverySearch_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": "rate limit exceeded"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.DiscoverySearch(context.Background(), DiscoverySearchRequest{TextQuery: "test"})

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "429")
}
