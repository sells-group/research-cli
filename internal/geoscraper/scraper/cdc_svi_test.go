package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// sviArcGISResponse builds a minimal ArcGIS JSON response for testing.
func sviArcGISResponse(features []map[string]any, exceeded bool) []byte {
	resp := map[string]any{
		"features":              features,
		"exceededTransferLimit": exceeded,
	}
	data, _ := json.Marshal(resp)
	return data
}

// expectSVIBulkUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.svi table.
func expectSVIBulkUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_svi"}, sviCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

func TestCDCSvi_Metadata(t *testing.T) {
	s := &CDCSvi{}
	assert.Equal(t, "cdc_svi", s.Name())
	assert.Equal(t, "geo.svi", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestCDCSvi_ShouldRun(t *testing.T) {
	s := &CDCSvi{}
	now := fixedNow()

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently → should not run.
	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestCDCSvi_Sync(t *testing.T) {
	features := []map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": "48453001100", "RPL_THEMES": 0.75, "RPL_THEME1": 0.6,
				"RPL_THEME2": 0.7, "RPL_THEME3": 0.8, "RPL_THEME4": 0.65,
				"E_TOTPOP": 5000.0, "E_POV150": 1200.0, "E_UNEMP": 300.0,
				"E_HBURD": 800.0, "E_NOHSDP": 150.0, "E_UNINSUR": 400.0,
				"EP_POV150": 24.0, "EP_UNEMP": 6.0, "EP_HBURD": 16.0,
				"EP_NOHSDP": 3.0, "EP_UNINSUR": 8.0,
			},
		},
		{
			"attributes": map[string]any{
				"FIPS": "48453001200", "RPL_THEMES": 0.55, "RPL_THEME1": 0.4,
				"RPL_THEME2": 0.5, "RPL_THEME3": 0.6, "RPL_THEME4": 0.45,
				"E_TOTPOP": 3000.0, "E_POV150": 600.0, "E_UNEMP": 100.0,
				"E_HBURD": 400.0, "E_NOHSDP": 80.0, "E_UNINSUR": 200.0,
				"EP_POV150": 20.0, "EP_UNEMP": 3.3, "EP_HBURD": 13.3,
				"EP_NOHSDP": 2.7, "EP_UNINSUR": 6.7,
			},
		},
		{
			// RPL_THEMES = -999 → null sentinel, should be skipped.
			"attributes": map[string]any{
				"FIPS": "48453001300", "RPL_THEMES": -999.0, "RPL_THEME1": -999.0,
				"RPL_THEME2": -999.0, "RPL_THEME3": -999.0, "RPL_THEME4": -999.0,
				"E_TOTPOP": 0.0, "E_POV150": 0.0, "E_UNEMP": 0.0,
				"E_HBURD": 0.0, "E_NOHSDP": 0.0, "E_UNINSUR": 0.0,
				"EP_POV150": 0.0, "EP_UNEMP": 0.0, "EP_HBURD": 0.0,
				"EP_NOHSDP": 0.0, "EP_UNINSUR": 0.0,
			},
		},
	}

	data := sviArcGISResponse(features, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Row 3 has RPL_THEMES=-999 (CDC null sentinel) and should be skipped → 2 rows.
	expectSVIBulkUpsert(mock, 2)

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCDCSvi_Pagination(t *testing.T) {
	page1 := sviArcGISResponse([]map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": "48453001100", "RPL_THEMES": 0.75, "RPL_THEME1": 0.6,
				"RPL_THEME2": 0.7, "RPL_THEME3": 0.8, "RPL_THEME4": 0.65,
				"E_TOTPOP": 5000.0, "E_POV150": 1200.0, "E_UNEMP": 300.0,
				"E_HBURD": 800.0, "E_NOHSDP": 150.0, "E_UNINSUR": 400.0,
				"EP_POV150": 24.0, "EP_UNEMP": 6.0, "EP_HBURD": 16.0,
				"EP_NOHSDP": 3.0, "EP_UNINSUR": 8.0,
			},
		},
	}, true) // exceededTransferLimit = true → more pages

	page2 := sviArcGISResponse([]map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": "48453001200", "RPL_THEMES": 0.55, "RPL_THEME1": 0.4,
				"RPL_THEME2": 0.5, "RPL_THEME3": 0.6, "RPL_THEME4": 0.45,
				"E_TOTPOP": 3000.0, "E_POV150": 600.0, "E_UNEMP": 100.0,
				"E_HBURD": 400.0, "E_NOHSDP": 80.0, "E_UNINSUR": 200.0,
				"EP_POV150": 20.0, "EP_UNEMP": 3.3, "EP_HBURD": 13.3,
				"EP_NOHSDP": 2.7, "EP_UNINSUR": 6.7,
			},
		},
	}, false) // last page

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		callCount++
		if r.URL.Query().Get("resultOffset") == "0" {
			_, _ = w.Write(page1)
		} else {
			_, _ = w.Write(page2)
		}
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Two pages, each with 1 row → single flush at end with 2 rows.
	expectSVIBulkUpsert(mock, 2)

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.Equal(t, 2, callCount, "expected 2 paginated requests")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCDCSvi_NullFIPS(t *testing.T) {
	features := []map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": nil, "RPL_THEMES": 0.5, "RPL_THEME1": 0.5,
				"RPL_THEME2": 0.5, "RPL_THEME3": 0.5, "RPL_THEME4": 0.5,
				"E_TOTPOP": 100.0, "E_POV150": 50.0, "E_UNEMP": 10.0,
				"E_HBURD": 20.0, "E_NOHSDP": 5.0, "E_UNINSUR": 10.0,
				"EP_POV150": 50.0, "EP_UNEMP": 10.0, "EP_HBURD": 20.0,
				"EP_NOHSDP": 5.0, "EP_UNINSUR": 10.0,
			},
		},
	}

	data := sviArcGISResponse(features, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCDCSvi_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &CDCSvi{baseURL: "http://127.0.0.1:1/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query arcgis")
}

func TestCDCSvi_EmptyResponse(t *testing.T) {
	data := sviArcGISResponse(nil, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCDCSvi_SviDataYear(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want int
	}{
		{"even year 2026", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), 2024},
		{"odd year 2025", time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), 2022},
		{"even year 2024", time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), 2022},
		{"odd year 2023", time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC), 2020},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sviDataYear(tt.now)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCDCSvi_SviAttrString(t *testing.T) {
	// String FIPS.
	attrs := map[string]any{"FIPS": "48453001100"}
	assert.Equal(t, "48453001100", sviAttrString(attrs, "FIPS"))

	// Numeric FIPS (ArcGIS may return as float64).
	attrs = map[string]any{"FIPS": 48453001100.0}
	assert.Equal(t, "48453001100", sviAttrString(attrs, "FIPS"))

	// Nil value.
	attrs = map[string]any{"FIPS": nil}
	assert.Equal(t, "", sviAttrString(attrs, "FIPS"))

	// Missing key.
	attrs = map[string]any{}
	assert.Equal(t, "", sviAttrString(attrs, "FIPS"))

	// json.Number value.
	attrs = map[string]any{"FIPS": json.Number("48453001100")}
	assert.Equal(t, "48453001100", sviAttrString(attrs, "FIPS"))

	// Default case (unsupported type) returns empty string.
	attrs = map[string]any{"FIPS": true}
	assert.Equal(t, "", sviAttrString(attrs, "FIPS"))

	attrs = map[string]any{"FIPS": []int{1, 2}}
	assert.Equal(t, "", sviAttrString(attrs, "FIPS"))
}

func TestCDCSvi_SviAttrInt(t *testing.T) {
	attrs := map[string]any{"E_TOTPOP": 5000.0}
	assert.Equal(t, 5000, sviAttrInt(attrs, "E_TOTPOP"))

	attrs = map[string]any{"E_TOTPOP": nil}
	assert.Equal(t, 0, sviAttrInt(attrs, "E_TOTPOP"))

	attrs = map[string]any{}
	assert.Equal(t, 0, sviAttrInt(attrs, "E_TOTPOP"))

	// json.Number value.
	attrs = map[string]any{"E_TOTPOP": json.Number("3000")}
	assert.Equal(t, 3000, sviAttrInt(attrs, "E_TOTPOP"))

	// Default case (unsupported type) returns 0.
	attrs = map[string]any{"E_TOTPOP": "not a number"}
	assert.Equal(t, 0, sviAttrInt(attrs, "E_TOTPOP"))

	attrs = map[string]any{"E_TOTPOP": true}
	assert.Equal(t, 0, sviAttrInt(attrs, "E_TOTPOP"))
}

func TestCDCSvi_Properties(t *testing.T) {
	// Extra fields beyond sviExclude should appear in properties.
	feat := arcgis.Feature{
		Attributes: map[string]any{
			"FIPS":       "48453001100",
			"RPL_THEMES": 0.75,
			"LOCATION":   "Austin, TX",
		},
	}
	props := hifldProperties(feat.Attributes, sviExclude)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(props, &parsed))
	assert.Equal(t, "Austin, TX", parsed["LOCATION"])
	_, hasFIPS := parsed["FIPS"]
	assert.False(t, hasFIPS, "FIPS should be excluded from properties")
}

func TestCDCSvi_UpsertError(t *testing.T) {
	features := []map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": "48453001100", "RPL_THEMES": 0.75, "RPL_THEME1": 0.6,
				"RPL_THEME2": 0.7, "RPL_THEME3": 0.8, "RPL_THEME4": 0.65,
				"E_TOTPOP": 5000.0, "E_POV150": 1200.0, "E_UNEMP": 300.0,
				"E_HBURD": 800.0, "E_NOHSDP": 150.0, "E_UNINSUR": 400.0,
				"EP_POV150": 24.0, "EP_UNEMP": 6.0, "EP_HBURD": 16.0,
				"EP_NOHSDP": 3.0, "EP_UNINSUR": 8.0,
			},
		},
	}
	data := sviArcGISResponse(features, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestCDCSvi_ShortFIPS(t *testing.T) {
	// FIPS with fewer than 5 characters → state_fips and county_fips should be empty.
	features := []map[string]any{
		{
			"attributes": map[string]any{
				"FIPS": "484", "RPL_THEMES": 0.75, "RPL_THEME1": 0.6,
				"RPL_THEME2": 0.7, "RPL_THEME3": 0.8, "RPL_THEME4": 0.65,
				"E_TOTPOP": 5000.0, "E_POV150": 1200.0, "E_UNEMP": 300.0,
				"E_HBURD": 800.0, "E_NOHSDP": 150.0, "E_UNINSUR": 400.0,
				"EP_POV150": 24.0, "EP_UNEMP": 6.0, "EP_HBURD": 16.0,
				"EP_NOHSDP": 3.0, "EP_UNINSUR": 8.0,
			},
		},
	}
	data := sviArcGISResponse(features, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectSVIBulkUpsert(mock, 1)

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

func TestCDCSvi_BatchOverflow(t *testing.T) {
	// Generate >5000 features to trigger batch flush mid-page.
	features := make([]map[string]any, 5002)
	for i := range features {
		features[i] = map[string]any{
			"attributes": map[string]any{
				"FIPS": fmt.Sprintf("48453%06d", i+1), "RPL_THEMES": 0.5,
				"RPL_THEME1": 0.5, "RPL_THEME2": 0.5, "RPL_THEME3": 0.5, "RPL_THEME4": 0.5,
				"E_TOTPOP": 100.0, "E_POV150": 50.0, "E_UNEMP": 10.0,
				"E_HBURD": 20.0, "E_NOHSDP": 5.0, "E_UNINSUR": 10.0,
				"EP_POV150": 50.0, "EP_UNEMP": 10.0, "EP_HBURD": 20.0,
				"EP_NOHSDP": 5.0, "EP_UNINSUR": 10.0,
			},
		}
	}
	data := sviArcGISResponse(features, false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch: 5000 rows.
	expectSVIBulkUpsert(mock, 5000)
	// Second batch: 2 remaining rows.
	expectSVIBulkUpsert(mock, 2)

	s := &CDCSvi{baseURL: srv.URL + "/query", year: 2022}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}
