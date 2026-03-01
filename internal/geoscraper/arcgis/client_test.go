package arcgis

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
)

func newTestFetcher() fetcher.Fetcher {
	return fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
}

func TestQueryAll_SinglePage(t *testing.T) {
	data, err := os.ReadFile("testdata/point_response.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	var collected []Feature
	err = QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: srv.URL + "/query",
	}, func(features []Feature) error {
		collected = append(collected, features...)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, collected, 2)
	assert.Equal(t, "Plant Alpha", collected[0].Attributes["NAME"])
}

func TestQueryAll_MultiPage(t *testing.T) {
	page1, err := os.ReadFile("testdata/multipage_1.json")
	require.NoError(t, err)
	page2, err := os.ReadFile("testdata/multipage_2.json")
	require.NoError(t, err)

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if callCount == 0 {
			_, _ = w.Write(page1)
		} else {
			_, _ = w.Write(page2)
		}
		callCount++
	}))
	defer srv.Close()

	var total int
	err = QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL:  srv.URL + "/query",
		PageSize: 2,
	}, func(features []Feature) error {
		total += len(features)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, callCount)
}

func TestQueryAll_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	var called bool
	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: srv.URL + "/query",
	}, func(_ []Feature) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, called)
}

func TestQueryAll_CancelledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[{"attributes":{"ID":1}}],"exceededTransferLimit":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())

	callCount := 0
	err := QueryAll(ctx, newTestFetcher(), QueryConfig{
		BaseURL: srv.URL + "/query",
	}, func(_ []Feature) error {
		callCount++
		cancel() // cancel after first page
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, 1, callCount)
}

func TestGeometry_Centroid_Point(t *testing.T) {
	x, y := -95.5, 30.2
	g := Geometry{X: &x, Y: &y}

	lat, lon := g.Centroid()
	assert.InDelta(t, 30.2, lat, 0.001)
	assert.InDelta(t, -95.5, lon, 0.001)
}

func TestGeometry_Centroid_Polyline(t *testing.T) {
	g := Geometry{
		Paths: [][][2]float64{
			{{-100.0, 30.0}, {-90.0, 40.0}},
		},
	}

	lat, lon := g.Centroid()
	assert.InDelta(t, 35.0, lat, 0.001)
	assert.InDelta(t, -95.0, lon, 0.001)
}

func TestGeometry_Centroid_MultiPath(t *testing.T) {
	g := Geometry{
		Paths: [][][2]float64{
			{{-100.0, 30.0}, {-90.0, 40.0}},
			{{-80.0, 30.0}, {-70.0, 40.0}},
		},
	}

	lat, lon := g.Centroid()
	// avg lat = (30+40+30+40)/4 = 35, avg lon = (-100-90-80-70)/4 = -85
	assert.InDelta(t, 35.0, lat, 0.001)
	assert.InDelta(t, -85.0, lon, 0.001)
}

func TestGeometry_Centroid_Empty(t *testing.T) {
	g := Geometry{}
	lat, lon := g.Centroid()
	assert.Equal(t, 0.0, lat)
	assert.Equal(t, 0.0, lon)
}

func TestGeometry_BBox(t *testing.T) {
	g := Geometry{
		Paths: [][][2]float64{
			{{-100.0, 30.0}, {-90.0, 40.0}},
		},
	}

	bbox := g.BBox()
	require.Len(t, bbox, 4)
	assert.InDelta(t, -100.0, bbox[0], 0.001) // minLon
	assert.InDelta(t, 30.0, bbox[1], 0.001)   // minLat
	assert.InDelta(t, -90.0, bbox[2], 0.001)  // maxLon
	assert.InDelta(t, 40.0, bbox[3], 0.001)   // maxLat
}

func TestGeometry_BBox_Point(t *testing.T) {
	x, y := -95.5, 30.2
	g := Geometry{X: &x, Y: &y}
	assert.Nil(t, g.BBox())
}

func TestFormatURL(t *testing.T) {
	got := FormatURL("Power_Plants")
	assert.Contains(t, got, "Power_Plants/FeatureServer/0/query")
	assert.Contains(t, got, "services1.arcgis.com")
}

func TestQueryAll_DownloadError(t *testing.T) {
	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: "http://127.0.0.1:1/nonexistent",
	}, func(_ []Feature) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download page")
}

func TestQueryAll_CustomOutFields(t *testing.T) {
	var receivedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[{"attributes":{"NAME":"A"}}],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL:   srv.URL + "/query",
		OutFields: []string{"NAME", "TYPE"},
		Where:     "STATE='TX'",
	}, func(_ []Feature) error {
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, receivedURL, "outFields=NAME%2CTYPE")
	assert.Contains(t, receivedURL, "where=STATE%3D%27TX%27")
}

func TestQueryAll_CallbackError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[{"attributes":{"ID":1}}],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: srv.URL + "/query",
	}, func(_ []Feature) error {
		return assert.AnError
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "callback error")
}

func TestQueryAll_InvalidBaseURL(t *testing.T) {
	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: "://bad-url",
	}, func(_ []Feature) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build query URL")
}

func TestQueryAll_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	err := QueryAll(context.Background(), newTestFetcher(), QueryConfig{
		BaseURL: srv.URL + "/query",
	}, func(_ []Feature) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}
