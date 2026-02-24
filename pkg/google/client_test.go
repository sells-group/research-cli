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
