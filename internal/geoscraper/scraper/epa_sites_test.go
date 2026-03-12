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

func TestEPASites_Metadata(t *testing.T) {
	s := &EPASites{}
	assert.Equal(t, "epa_sites", s.Name())
	assert.Equal(t, "geo.epa_sites", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Monthly, s.Cadence())
}

func TestEPASites_ShouldRun(t *testing.T) {
	s := &EPASites{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (same month, March 1 mid-day) → should not run.
	recent := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// Synced last month → should run.
	stale := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

func TestEPASites_ParseFeature(t *testing.T) {
	x, y := -97.7431, 30.2672
	feat := arcgis.Feature{
		Attributes: map[string]any{
			"OBJECTID":         1001.0,
			"REGISTRY_ID":      "110000307365",
			"PRIMARY_NAME":     "Acme Chemical Plant",
			"PGM_SYS_ACRNM":    "RCRAINFO",
			"ACTIVE_STATUS":    "ACTIVE",
			"LATITUDE83":       30.2672,
			"LONGITUDE83":      -97.7431,
			"LOCATION_ADDRESS": "123 Main St",
			"CITY_NAME":        "Austin",
			"COUNTY_NAME":      "Travis",
			"STATE_CODE":       "TX",
			"POSTAL_CODE":      "78701",
			"EPA_REGION_CODE":  "06",
			"INTEREST_TYPE":    "RCRA-LQG",
			"FIPS_CODE":        "48453",
		},
		Geometry: &arcgis.Geometry{X: &x, Y: &y},
	}

	row, ok := newEPARow(feat)
	require.True(t, ok)
	require.Len(t, row, 9)

	assert.Equal(t, "Acme Chemical Plant", row[0]) // name
	assert.Equal(t, "RCRAINFO", row[1])            // program
	assert.Equal(t, "110000307365", row[2])        // registry_id
	assert.Equal(t, "ACTIVE", row[3])              // status
	assert.InDelta(t, 30.2672, row[4], 0.001)      // latitude
	assert.InDelta(t, -97.7431, row[5], 0.001)     // longitude
	assert.Equal(t, epaSource, row[6])             // source
	assert.Equal(t, "110000307365", row[7])        // source_id

	// Verify properties exclude dedicated columns.
	var props map[string]any
	err := json.Unmarshal(row[8].([]byte), &props)
	require.NoError(t, err)
	assert.Contains(t, props, "LOCATION_ADDRESS")
	assert.Contains(t, props, "CITY_NAME")
	assert.Contains(t, props, "COUNTY_NAME")
	assert.Contains(t, props, "STATE_CODE")
	assert.Contains(t, props, "EPA_REGION_CODE")
	assert.NotContains(t, props, "OBJECTID")
	assert.NotContains(t, props, "REGISTRY_ID")
	assert.NotContains(t, props, "PRIMARY_NAME")
	assert.NotContains(t, props, "PGM_SYS_ACRNM")
	assert.NotContains(t, props, "ACTIVE_STATUS")
	assert.NotContains(t, props, "LATITUDE83")
	assert.NotContains(t, props, "LONGITUDE83")
}

func TestEPASites_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/epa_sites.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 56 states × 2 features each = 112 rows in a single batch.
	totalFeatures := int64(2 * len(stateAbbrevs))
	expectEPAUpsert(mock, totalFeatures)

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEPASites_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{
				"attributes": {"OBJECTID": 1, "REGISTRY_ID": "100000000001", "PRIMARY_NAME": "Good", "PGM_SYS_ACRNM": "RCRA", "ACTIVE_STATUS": "ACTIVE", "LATITUDE83": 30.0, "LONGITUDE83": -95.0},
				"geometry": {"x": -95.0, "y": 30.0}
			},
			{
				"attributes": {"OBJECTID": 2, "REGISTRY_ID": "100000000002", "PRIMARY_NAME": "Bad", "PGM_SYS_ACRNM": "RCRA", "ACTIVE_STATUS": "ACTIVE", "LATITUDE83": 30.0, "LONGITUDE83": -95.0},
				"geometry": null
			}
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

	// 56 states × 1 valid feature each = 56 rows in a single batch.
	totalFeatures := int64(len(stateAbbrevs))
	expectEPAUpsert(mock, totalFeatures)

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
}

func TestEPASites_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestEPASites_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/epa_sites.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First state's upsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestEPASites_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EPASites{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestEPASites_BatchOverflow(t *testing.T) {
	// Generate >5000 features to trigger batch flush mid-page.
	features := make([]map[string]any, 5002)
	for i := range features {
		features[i] = map[string]any{
			"attributes": map[string]any{
				"OBJECTID":      float64(i + 1),
				"REGISTRY_ID":   fmt.Sprintf("1100003%05d", i+1),
				"PRIMARY_NAME":  fmt.Sprintf("Facility %d", i+1),
				"PGM_SYS_ACRNM": "RCRA",
				"ACTIVE_STATUS": "ACTIVE",
				"LATITUDE83":    30.0,
				"LONGITUDE83":   -95.0,
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

	// Only serve data for the first state, empty for the rest.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if callCount == 0 {
			_, _ = w.Write(data)
		} else {
			_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
		}
		callCount++
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch: 5000 rows (mid-page flush).
	expectEPAUpsert(mock, 5000)
	// Second batch: 2 remaining rows (end-of-states flush).
	expectEPAUpsert(mock, 2)

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEPASites_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestEPASites_SkipMissingRegistryID(t *testing.T) {
	data := []byte(`{
		"features": [
			{
				"attributes": {"OBJECTID": 1, "REGISTRY_ID": "100000000001", "PRIMARY_NAME": "Good", "PGM_SYS_ACRNM": "RCRA", "ACTIVE_STATUS": "ACTIVE"},
				"geometry": {"x": -95.0, "y": 30.0}
			},
			{
				"attributes": {"OBJECTID": 2, "PRIMARY_NAME": "No Registry ID", "PGM_SYS_ACRNM": "RCRA", "ACTIVE_STATUS": "ACTIVE"},
				"geometry": {"x": -95.0, "y": 30.0}
			}
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

	// 56 states × 1 valid feature each = 56 rows.
	totalFeatures := int64(len(stateAbbrevs))
	expectEPAUpsert(mock, totalFeatures)

	s := &EPASites{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
}

// expectEPAUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.epa_sites table.
func expectEPAUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_epa_sites"}, epaCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
