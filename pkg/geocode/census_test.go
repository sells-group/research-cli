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

func TestCensusSingleGeocode_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"result": {
				"addressMatches": [{
					"coordinates": {"x": -77.0365, "y": 38.8977},
					"matchedAddress": "1600 Pennsylvania Ave NW, Washington, DC 20500",
					"tigerLine": {"side": "L", "tigerLineId": "123"}
				}]
			}
		}`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: srv.Client(),
		limiter:    newTestLimiter(),
	}
	// Override Census URL via a custom transport.
	g.httpClient = newRewriteClient(srv.URL, censusOneLineURL)

	result, err := g.geocodeCensus(context.Background(), AddressInput{
		Street: "1600 Pennsylvania Ave NW", City: "Washington", State: "DC", ZipCode: "20500",
	})
	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.InDelta(t, 38.8977, result.Latitude, 0.0001)
	assert.InDelta(t, -77.0365, result.Longitude, 0.0001)
	assert.Equal(t, "census", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
}

func TestCensusSingleGeocode_NoMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"result": {"addressMatches": []}}`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, censusOneLineURL),
		limiter:    newTestLimiter(),
	}

	result, err := g.geocodeCensus(context.Background(), AddressInput{
		Street: "123 Nowhere St", City: "Faketown", State: "XX", ZipCode: "00000",
	})
	require.NoError(t, err)
	assert.False(t, result.Matched)
	assert.Equal(t, "census", result.Source)
}

func TestCensusBatch_MixedResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Batch response CSV: id,"input","Match/No Match","Exact/Non_Exact","matched addr",lon/lat,tiger,side
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, `"0","1600 Pennsylvania Ave NW, Washington, DC, 20500","Match","Exact","1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500","-77.0365,38.8977","123","L"
"1","123 Nowhere St, Faketown, XX, 00000","No_Match"`)
	}))
	defer srv.Close()

	g := &geocoder{
		httpClient: newRewriteClient(srv.URL, censusBatchURL),
		limiter:    newTestLimiter(),
	}

	addrs := []AddressInput{
		{ID: "0", Street: "1600 Pennsylvania Ave NW", City: "Washington", State: "DC", ZipCode: "20500"},
		{ID: "1", Street: "123 Nowhere St", City: "Faketown", State: "XX", ZipCode: "00000"},
	}

	results, err := g.batchGeocodeCensus(context.Background(), addrs)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.True(t, results[0].Matched)
	assert.InDelta(t, 38.8977, results[0].Latitude, 0.0001)
	assert.InDelta(t, -77.0365, results[0].Longitude, 0.0001)
	assert.Equal(t, "census", results[0].Source)
	assert.Equal(t, "rooftop", results[0].Quality)

	assert.False(t, results[1].Matched)
}

func TestParseCensusBatchResponse(t *testing.T) {
	body := `"0","input addr","Match","Non_Exact","matched","-73.9857,40.7484","999","R"
"1","input addr","No_Match"`

	idToIdx := map[string]int{"0": 0, "1": 1}
	results, err := parseCensusBatchResponse(body, idToIdx, 2)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.True(t, results[0].Matched)
	assert.Equal(t, "range", results[0].Quality) // Non_Exact -> range
	assert.InDelta(t, 40.7484, results[0].Latitude, 0.0001)
	assert.InDelta(t, -73.9857, results[0].Longitude, 0.0001)

	assert.False(t, results[1].Matched)
}

func TestFormatOneLine(t *testing.T) {
	tests := []struct {
		addr     AddressInput
		expected string
	}{
		{
			AddressInput{Street: "123 Main St", City: "Springfield", State: "IL", ZipCode: "62701"},
			"123 Main St, Springfield, IL, 62701",
		},
		{
			AddressInput{Street: "456 Oak Ave", City: "Portland", State: "OR"},
			"456 Oak Ave, Portland, OR",
		},
		{
			AddressInput{City: "Denver", State: "CO", ZipCode: "80202"},
			"Denver, CO, 80202",
		},
	}

	for _, tt := range tests {
		result := formatOneLine(tt.addr)
		assert.Equal(t, tt.expected, result)
	}
}
