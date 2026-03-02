package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

func fixedNow() time.Time {
	return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
}

func TestPowerPlants_Metadata(t *testing.T) {
	s := &HIFLDPowerPlants{}
	assert.Equal(t, "hifld_power_plants", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestPowerPlants_ShouldRun(t *testing.T) {
	s := &HIFLDPowerPlants{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (same quarter) → should not run.
	recent := now.Add(-24 * time.Hour)
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestPowerPlants_ParseFeature(t *testing.T) {
	x, y := -97.7431, 30.2672
	feat := arcgis.Feature{
		Attributes: map[string]any{
			"OBJECTID":    101.0,
			"NAME":        "Sunrise Solar Farm",
			"TYPE":        "SOLAR",
			"CAPACITY_MW": 250.0,
			"STATE":       "TX",
		},
		Geometry: &arcgis.Geometry{X: &x, Y: &y},
	}

	lat, lon := feat.Geometry.Centroid()
	assert.InDelta(t, 30.2672, lat, 0.001)
	assert.InDelta(t, -97.7431, lon, 0.001)
	assert.Equal(t, "Sunrise Solar Farm", hifldString(feat.Attributes, "NAME"))
	assert.Equal(t, "SOLAR", hifldString(feat.Attributes, "TYPE"))
	assert.InDelta(t, 250.0, hifldFloat64(feat.Attributes, "CAPACITY_MW"), 0.001)
}

func TestPowerPlants_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/power_plants.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &HIFLDPowerPlants{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPowerPlants_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{"attributes": {"OBJECTID": 1, "NAME": "Good"}, "geometry": {"x": -95.0, "y": 30.0}},
			{"attributes": {"OBJECTID": 2, "NAME": "Bad"}, "geometry": null}
		],
		"exceededTransferLimit": false
	}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &HIFLDPowerPlants{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestPowerPlants_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/power_plants.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// BulkUpsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &HIFLDPowerPlants{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestPowerPlants_QueryError(t *testing.T) {
	// Point at a nonexistent server to trigger a download error.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDPowerPlants{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}

func TestPowerPlants_BatchOverflow(t *testing.T) {
	// Generate >5000 features to trigger batch flush mid-page.
	features := make([]map[string]any, 5002)
	for i := range features {
		features[i] = map[string]any{
			"attributes": map[string]any{
				"OBJECTID":    float64(i + 1),
				"NAME":        fmt.Sprintf("Plant %d", i+1),
				"TYPE":        "GAS",
				"CAPACITY_MW": 100.0,
			},
			"geometry": map[string]any{
				"x": -95.0,
				"y": 30.0,
			},
		}
	}
	resp := map[string]any{
		"features":              features,
		"exceededTransferLimit": false,
	}
	data, err := json.Marshal(resp)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch: 5000 rows.
	expectBulkUpsert(mock, 5000)
	// Second batch: 2 remaining rows.
	expectBulkUpsert(mock, 2)

	s := &HIFLDPowerPlants{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPowerPlants_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HIFLDPowerPlants{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// expectBulkUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.infrastructure table.
func expectBulkUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_infrastructure"}, infraCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
