package overpass

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testOverpassJSON = `{
	"elements": [
		{"type": "node", "id": 123, "lat": 40.7128, "lon": -74.006, "tags": {"amenity": "school", "name": "PS 101"}},
		{"type": "node", "id": 456, "lat": 40.7580, "lon": -73.9855, "tags": {"amenity": "hospital", "name": "NYC Hospital"}},
		{"type": "node", "id": 789, "lat": 40.7484, "lon": -73.9857, "tags": {"leisure": "park", "name": "Bryant Park"}}
	]
}`

func TestQuery_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(testOverpassJSON))
	}))
	defer srv.Close()

	elems, err := Query(context.Background(), srv.URL, "[out:json];node(1);out;")
	require.NoError(t, err)
	require.Len(t, elems, 3)

	assert.Equal(t, int64(123), elems[0].ID)
	assert.Equal(t, "node", elems[0].Type)
	assert.InDelta(t, 40.7128, elems[0].Lat, 0.001)
	assert.InDelta(t, -74.006, elems[0].Lon, 0.001)
	assert.Equal(t, "school", elems[0].Tags["amenity"])
	assert.Equal(t, "PS 101", elems[0].Tags["name"])

	assert.Equal(t, int64(456), elems[1].ID)
	assert.Equal(t, "hospital", elems[1].Tags["amenity"])

	assert.Equal(t, int64(789), elems[2].ID)
	assert.Equal(t, "park", elems[2].Tags["leisure"])
	assert.Equal(t, "Bryant Park", elems[2].Tags["name"])
}

func TestQuery_DefaultEndpoint(t *testing.T) {
	assert.Equal(t, "https://overpass-api.de/api/interpreter", DefaultEndpoint)
}

func TestBuildPOIQuery(t *testing.T) {
	q := BuildPOIQuery(24.396308, -125.0, 49.384358, -66.93457)
	assert.Contains(t, q, "24.396308,-125.000000,49.384358,-66.934570")
	assert.Contains(t, q, `"amenity"`)
	assert.Contains(t, q, "school")
	assert.Contains(t, q, "hospital")
	assert.Contains(t, q, "fire_station")
	assert.Contains(t, q, "police")
	assert.Contains(t, q, "library")
	assert.Contains(t, q, "post_office")
	assert.Contains(t, q, "place_of_worship")
	assert.Contains(t, q, `"leisure"="park"`)
	assert.Contains(t, q, "[out:json][timeout:120]")
}

func TestQuery_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server overloaded", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := Query(context.Background(), srv.URL, "[out:json];node(1);out;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestQuery_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	_, err := Query(context.Background(), srv.URL, "[out:json];node(1);out;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestQuery_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(testOverpassJSON))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Query(ctx, srv.URL, "[out:json];node(1);out;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execute request")
}

func TestQuery_PostsFormData(t *testing.T) {
	var receivedBody string
	var receivedContentType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"elements":[]}`))
	}))
	defer srv.Close()

	_, err := Query(context.Background(), srv.URL, "[out:json];node(1);out;")
	require.NoError(t, err)
	assert.Equal(t, "application/x-www-form-urlencoded", receivedContentType)
	assert.Contains(t, receivedBody, "data=")
}
