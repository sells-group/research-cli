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

func TestFEMAFlood_Metadata(t *testing.T) {
	s := &FEMAFloodZones{}
	assert.Equal(t, "fema_flood", s.Name())
	assert.Equal(t, "geo.flood_zones", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Monthly, s.Cadence())
}

func TestFEMAFlood_ShouldRun(t *testing.T) {
	s := &FEMAFloodZones{}
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

func TestFEMAFlood_ParseFeature(t *testing.T) {
	feat := arcgis.Feature{
		Attributes: map[string]any{
			"OBJECTID":   501.0,
			"FLD_ZONE":   "AE",
			"FLD_AR_ID":  "48201C0100F",
			"DFIRM_ID":   "48201C",
			"SFHA_TF":    "T",
			"STATIC_BFE": 35.2,
			"ZONE_SUBTY": "FLOODWAY",
		},
		Geometry: &arcgis.Geometry{
			Rings: [][][2]float64{
				{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.8}, {-95.4, 29.7}},
			},
		},
	}

	row, ok := newFloodRow(feat)
	require.True(t, ok)
	require.Len(t, row, 6)

	assert.Equal(t, "AE", row[0])           // zone_code
	assert.Equal(t, "high_risk", row[1])    // flood_type
	assert.Contains(t, row[2], "SRID=4326") // geom_wkt
	assert.Contains(t, row[2], "MULTIPOLYGON")
	assert.Equal(t, femaSource, row[3])    // source
	assert.Equal(t, "48201C0100F", row[4]) // source_id

	// Verify properties exclude dedicated columns.
	var props map[string]any
	err := json.Unmarshal(row[5].([]byte), &props)
	require.NoError(t, err)
	assert.Contains(t, props, "STATIC_BFE")
	assert.Contains(t, props, "DFIRM_ID")
	assert.NotContains(t, props, "OBJECTID")
	assert.NotContains(t, props, "FLD_ZONE")
	assert.NotContains(t, props, "FLD_AR_ID")
}

func TestFEMAFlood_Sync(t *testing.T) {
	data, err := os.ReadFile("testdata/fema_flood.json")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch accumulates across all states and flushes once at the end.
	// 56 states × 2 features each = 112 rows in a single batch.
	totalFeatures := int64(2 * len(stateFIPS))
	expectFloodUpsert(mock, totalFeatures)

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFEMAFlood_NullGeometry(t *testing.T) {
	data := []byte(`{
		"features": [
			{
				"attributes": {"OBJECTID": 1, "FLD_ZONE": "AE", "FLD_AR_ID": "01001C0001F", "SFHA_TF": "T", "ZONE_SUBTY": ""},
				"geometry": {"rings": [[[-84.3, 33.7], [-84.2, 33.7], [-84.2, 33.8], [-84.3, 33.8], [-84.3, 33.7]]]}
			},
			{
				"attributes": {"OBJECTID": 2, "FLD_ZONE": "AE", "FLD_AR_ID": "01001C0002F", "SFHA_TF": "T", "ZONE_SUBTY": ""},
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
	totalFeatures := int64(len(stateFIPS))
	expectFloodUpsert(mock, totalFeatures)

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
}

func TestFEMAFlood_NoRings(t *testing.T) {
	data := []byte(`{
		"features": [
			{
				"attributes": {"OBJECTID": 1, "FLD_ZONE": "AE", "FLD_AR_ID": "01001C0001F", "SFHA_TF": "T", "ZONE_SUBTY": ""},
				"geometry": {"rings": []}
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

	// No features with valid geometry → no upsert calls.
	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFEMAFlood_UpsertError(t *testing.T) {
	data, err := os.ReadFile("testdata/fema_flood.json")
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

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFEMAFlood_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FEMAFloodZones{baseURL: "http://127.0.0.1:1/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFEMAFlood_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFEMAFlood_BatchOverflow(t *testing.T) {
	// Generate >5000 features to trigger batch flush mid-page.
	features := make([]map[string]any, 5002)
	for i := range features {
		features[i] = map[string]any{
			"attributes": map[string]any{
				"OBJECTID":   float64(i + 1),
				"FLD_ZONE":   "AE",
				"FLD_AR_ID":  fmt.Sprintf("48201C%04dF", i+1),
				"DFIRM_ID":   "48201C",
				"SFHA_TF":    "T",
				"ZONE_SUBTY": "",
			},
			"geometry": map[string]any{
				"rings": [][][2]float64{
					{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.8}, {-95.4, 29.7}},
				},
			},
		}
	}
	resp := map[string]any{
		"features":              features,
		"exceededTransferLimit": false,
	}
	data, err := json.Marshal(resp)
	require.NoError(t, err)

	// Only serve data for the first state (FIPS "01"), empty for the rest.
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
	expectFloodUpsert(mock, 5000)
	// Second batch: 2 remaining rows (end-of-states flush).
	expectFloodUpsert(mock, 2)

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFloodUpsert_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	n, err := floodUpsert(context.Background(), mock, "geo.flood_zones", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestFloodUpsert_CreateTempTableError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = floodUpsert(context.Background(), mock, "geo.flood_zones", sampleFloodBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create temp table")
}

func TestFloodUpsert_CopyError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_flood_zones"}, floodCols).WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = floodUpsert(context.Background(), mock, "geo.flood_zones", sampleFloodBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY into temp table")
}

func TestFloodUpsert_DedupError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_flood_zones"}, floodCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = floodUpsert(context.Background(), mock, "geo.flood_zones", sampleFloodBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dedup temp table")
}

func TestFloodUpsert_InsertError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_flood_zones"}, floodCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = floodUpsert(context.Background(), mock, "geo.flood_zones", sampleFloodBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INSERT ON CONFLICT")
}

func TestFloodUpsert_CommitError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_flood_zones"}, floodCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit().WillReturnError(assert.AnError)

	_, err = floodUpsert(context.Background(), mock, "geo.flood_zones", sampleFloodBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit tx")
}

func TestFEMAFlood_ContextCancelled(t *testing.T) {
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

	s := &FEMAFloodZones{baseURL: srv.URL + "/query"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

// sampleFloodBatch returns a minimal batch for floodUpsert testing.
func sampleFloodBatch() [][]any {
	return [][]any{
		{"AE", "high_risk", "SRID=4326;MULTIPOLYGON(((-95.4 29.7,-95.3 29.7,-95.3 29.8,-95.4 29.7)))", "fema", "test001", []byte("{}")},
	}
}

// expectFloodUpsert sets up pgxmock expectations for a single floodUpsert call.
func expectFloodUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_flood_zones"}, floodCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
