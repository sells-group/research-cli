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

// ---------- Metadata ----------

func TestCensusDemographics_Metadata(t *testing.T) {
	s := &CensusDemographics{}
	assert.Equal(t, "census_demographics", s.Name())
	assert.Equal(t, "geo.demographics", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

// ---------- ShouldRun ----------

func TestCensusDemographics_ShouldRun(t *testing.T) {
	s := &CensusDemographics{}
	now := fixedNow() // 2026-03-01 12:00 UTC

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (after January 1 of this year) → should not run.
	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// Synced last year (before January 1 of this year) → should run.
	stale := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

// ---------- parseACSResponse ----------

func TestParseACSResponse_HappyPath(t *testing.T) {
	data, err := os.ReadFile("testdata/census_acs.json")
	require.NoError(t, err)

	result, err := parseACSResponse(data)
	require.NoError(t, err)
	require.Len(t, result, 2)

	dr := result["48201950100"]
	require.NotNil(t, dr)
	assert.Equal(t, "48201950100", dr.geoid)
	require.NotNil(t, dr.population)
	assert.Equal(t, 5000, *dr.population)
	require.NotNil(t, dr.income)
	assert.InDelta(t, 75000.0, *dr.income, 0.1)
	require.NotNil(t, dr.age)
	assert.InDelta(t, 35.2, *dr.age, 0.1)
	require.NotNil(t, dr.housing)
	assert.Equal(t, 2000, *dr.housing)
}

func TestParseACSResponse_Empty(t *testing.T) {
	// Only header row, no data.
	data := []byte(`[["B01003_001E","B19013_001E","B01002_001E","B25001_001E","state","county","tract"]]`)
	result, err := parseACSResponse(data)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestParseACSResponse_MissingValues(t *testing.T) {
	// Census uses -666666666 for missing values.
	data := []byte(`[
		["B01003_001E","B19013_001E","B01002_001E","B25001_001E","state","county","tract"],
		["5000","-666666666","-666666666.0","2000","48","201","950100"]
	]`)
	result, err := parseACSResponse(data)
	require.NoError(t, err)
	require.Len(t, result, 1)

	dr := result["48201950100"]
	require.NotNil(t, dr)
	require.NotNil(t, dr.population)
	assert.Equal(t, 5000, *dr.population)
	assert.Nil(t, dr.income, "should be nil for -666666666")
	assert.Nil(t, dr.age, "should be nil for -666666666.0")
	require.NotNil(t, dr.housing)
	assert.Equal(t, 2000, *dr.housing)
}

func TestParseACSResponse_InvalidJSON(t *testing.T) {
	_, err := parseACSResponse([]byte(`not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse ACS response")
}

// ---------- newDemoRow ----------

func TestNewDemoRow_HappyPath(t *testing.T) {
	pop := 5000
	inc := 75000.0
	age := 35.2
	housing := 2000
	demoMap := map[string]*demoRow{
		"48201950100": {
			geoid:      "48201950100",
			population: &pop,
			income:     &inc,
			age:        &age,
			housing:    &housing,
		},
	}

	feat := arcgis.Feature{
		Attributes: map[string]any{
			"GEOID": "48201950100",
			"STATE": "48",
		},
		Geometry: &arcgis.Geometry{
			Rings: [][][2]float64{
				{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.8}, {-95.4, 29.7}},
			},
		},
	}

	row, ok := newDemoRow(feat, demoMap, 2023)
	require.True(t, ok)
	require.Len(t, row, 11)

	assert.Equal(t, "48201950100", row[0])     // geoid
	assert.Equal(t, censusGeoLevel, row[1])    // geo_level
	assert.Equal(t, 2023, row[2])              // year
	assert.Equal(t, &pop, row[3])              // total_population
	assert.Equal(t, &inc, row[4])              // median_income
	assert.Equal(t, &age, row[5])              // median_age
	assert.Equal(t, &housing, row[6])          // housing_units
	assert.Contains(t, row[7], "SRID=4326")    // geom_wkt
	assert.Contains(t, row[7], "MULTIPOLYGON") // geom_wkt
	assert.Equal(t, censusSource, row[8])      // source
	assert.Equal(t, "48201950100", row[9])     // source_id
}

func TestNewDemoRow_NoGeometry(t *testing.T) {
	demoMap := map[string]*demoRow{"48201950100": {geoid: "48201950100"}}
	feat := arcgis.Feature{
		Attributes: map[string]any{"GEOID": "48201950100"},
		Geometry:   nil,
	}
	_, ok := newDemoRow(feat, demoMap, 2023)
	assert.False(t, ok)
}

func TestNewDemoRow_NoGEOID(t *testing.T) {
	demoMap := map[string]*demoRow{"48201950100": {geoid: "48201950100"}}
	feat := arcgis.Feature{
		Attributes: map[string]any{"STATE": "48"},
		Geometry: &arcgis.Geometry{
			Rings: [][][2]float64{{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.7}}},
		},
	}
	_, ok := newDemoRow(feat, demoMap, 2023)
	assert.False(t, ok)
}

func TestNewDemoRow_NoMatchingACS(t *testing.T) {
	demoMap := map[string]*demoRow{"99999999999": {geoid: "99999999999"}}
	feat := arcgis.Feature{
		Attributes: map[string]any{"GEOID": "48201950100"},
		Geometry: &arcgis.Geometry{
			Rings: [][][2]float64{{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.7}}},
		},
	}
	_, ok := newDemoRow(feat, demoMap, 2023)
	assert.False(t, ok)
}

func TestNewDemoRow_EmptyRings(t *testing.T) {
	demoMap := map[string]*demoRow{"48201950100": {geoid: "48201950100"}}
	feat := arcgis.Feature{
		Attributes: map[string]any{"GEOID": "48201950100"},
		Geometry:   &arcgis.Geometry{Rings: [][][2]float64{}},
	}
	_, ok := newDemoRow(feat, demoMap, 2023)
	assert.False(t, ok)
}

// ---------- demoUpsert error paths ----------

func TestDemoUpsert_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	n, err := demoUpsert(context.Background(), mock, "geo.demographics", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestDemoUpsert_BeginError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

func TestDemoUpsert_CreateTempTableError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create temp table")
}

func TestDemoUpsert_CopyError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_demographics"}, demoCols).WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "COPY into temp table")
}

func TestDemoUpsert_DedupError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_demographics"}, demoCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dedup temp table")
}

func TestDemoUpsert_InsertError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_demographics"}, demoCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnError(assert.AnError)
	mock.ExpectRollback()

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INSERT ON CONFLICT")
}

func TestDemoUpsert_CommitError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_demographics"}, demoCols).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit().WillReturnError(assert.AnError)

	_, err = demoUpsert(context.Background(), mock, "geo.demographics", sampleDemoBatch())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit tx")
}

// ---------- Full Sync ----------

func TestCensusDemographics_Sync(t *testing.T) {
	acsData, err := os.ReadFile("testdata/census_acs.json")
	require.NoError(t, err)
	tigerData, err := os.ReadFile("testdata/census_tracts.json")
	require.NoError(t, err)

	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(acsData)
	}))
	defer acsSrv.Close()

	tigerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(tigerData)
	}))
	defer tigerSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 56 states × 2 matching tracts = 112 rows in a single batch.
	totalFeatures := int64(2 * len(stateFIPS))
	expectDemoUpsert(mock, totalFeatures)

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: tigerSrv.URL + "/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---------- Error paths ----------

func TestCensusDemographics_MissingAPIKey(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CensusDemographics{}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Census API key is required")
}

func TestCensusDemographics_ACSError(t *testing.T) {
	// ACS returns invalid JSON; scraper should skip the state (non-fatal) and produce 0 rows.
	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not valid json`))
	}))
	defer acsSrv.Close()

	tigerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer tigerSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: tigerSrv.URL + "/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestCensusDemographics_TIGERwebError(t *testing.T) {
	acsData, err := os.ReadFile("testdata/census_acs.json")
	require.NoError(t, err)

	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(acsData)
	}))
	defer acsSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: "http://127.0.0.1:1/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TIGERweb query state")
}

func TestCensusDemographics_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   "http://127.0.0.1:1",
		tigerBaseURL: "http://127.0.0.1:1/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

// ---------- Edge cases ----------

func TestCensusDemographics_EmptyResponse(t *testing.T) {
	// ACS returns only header → empty demoMap → no TIGERweb queries needed.
	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[["B01003_001E","B19013_001E","B01002_001E","B25001_001E","state","county","tract"]]`))
	}))
	defer acsSrv.Close()

	tigerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
	}))
	defer tigerSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: tigerSrv.URL + "/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestCensusDemographics_BatchOverflow(t *testing.T) {
	// Build ACS response with >5000 tracts for state 48.
	acsRows := make([][]string, 5003) // header + 5002 data rows
	acsRows[0] = []string{"B01003_001E", "B19013_001E", "B01002_001E", "B25001_001E", "state", "county", "tract"}
	for i := 1; i <= 5002; i++ {
		tract := fmt.Sprintf("%06d", i)
		acsRows[i] = []string{"5000", "75000", "35.2", "2000", "48", "201", tract}
	}
	acsData, err := json.Marshal(acsRows)
	require.NoError(t, err)

	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(acsData)
	}))
	defer acsSrv.Close()

	// Build matching TIGERweb response.
	features := make([]map[string]any, 5002)
	for i := range features {
		tract := fmt.Sprintf("%06d", i+1)
		geoid := "48201" + tract
		features[i] = map[string]any{
			"attributes": map[string]any{
				"OBJECTID": float64(i + 1),
				"GEOID":    geoid,
				"STATE":    "48",
				"COUNTY":   "201",
				"TRACT":    tract,
			},
			"geometry": map[string]any{
				"rings": [][][2]float64{
					{{-95.4, 29.7}, {-95.3, 29.7}, {-95.3, 29.8}, {-95.4, 29.8}, {-95.4, 29.7}},
				},
			},
		}
	}

	// Only serve TIGERweb data for the first request (first state), empty for the rest.
	tigerCallCount := 0
	tigerResp := map[string]any{
		"features":              features,
		"exceededTransferLimit": false,
	}
	tigerData, err := json.Marshal(tigerResp)
	require.NoError(t, err)

	tigerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if tigerCallCount == 0 {
			_, _ = w.Write(tigerData)
		} else {
			_, _ = w.Write([]byte(`{"features":[],"exceededTransferLimit":false}`))
		}
		tigerCallCount++
	}))
	defer tigerSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch: 5000 rows (mid-page flush).
	expectDemoUpsert(mock, 5000)
	// Second batch: 2 remaining rows (end-of-states flush).
	expectDemoUpsert(mock, 2)

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: tigerSrv.URL + "/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCensusDemographics_UpsertError(t *testing.T) {
	acsData, err := os.ReadFile("testdata/census_acs.json")
	require.NoError(t, err)
	tigerData, err := os.ReadFile("testdata/census_tracts.json")
	require.NoError(t, err)

	acsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(acsData)
	}))
	defer acsSrv.Close()

	tigerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(tigerData)
	}))
	defer tigerSrv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Upsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &CensusDemographics{
		apiKey:       "test-key",
		acsBaseURL:   acsSrv.URL,
		tigerBaseURL: tigerSrv.URL + "/query",
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

// ---------- Helper method defaults ----------

func TestCensusDemographics_DefaultYear(t *testing.T) {
	s := &CensusDemographics{}
	assert.Equal(t, censusACSYear, s.acsYear())

	s2 := &CensusDemographics{year: 2022}
	assert.Equal(t, 2022, s2.acsYear())
}

func TestCensusDemographics_DefaultURLs(t *testing.T) {
	s := &CensusDemographics{}
	assert.Contains(t, s.acsURL(), "api.census.gov")
	assert.Equal(t, defaultTigerBaseURL, s.tigerURL())

	s2 := &CensusDemographics{acsBaseURL: "http://test/acs", tigerBaseURL: "http://test/tiger"}
	assert.Equal(t, "http://test/acs", s2.acsURL())
	assert.Equal(t, "http://test/tiger", s2.tigerURL())
}

// ---------- buildACSURL ----------

func TestBuildACSURL_WithKey(t *testing.T) {
	u := buildACSURL("http://api.census.gov/data/2023/acs/acs5", "mykey", "48")
	assert.Contains(t, u, "key=mykey")
	assert.Contains(t, u, "in=state:48")
	assert.Contains(t, u, "for=tract:*")
}

func TestBuildACSURL_WithoutKey(t *testing.T) {
	u := buildACSURL("http://api.census.gov/data/2023/acs/acs5", "", "06")
	assert.NotContains(t, u, "key=")
	assert.Contains(t, u, "in=state:06")
}

// ---------- Parse helper edge cases ----------

func TestSafeIndex_OutOfBounds(t *testing.T) {
	row := []string{"a", "b"}
	idx := map[string]int{"x": 5} // index beyond row length
	assert.Equal(t, "", safeIndex(row, idx, "x"))
}

func TestSafeIndex_MissingKey(t *testing.T) {
	row := []string{"a", "b"}
	idx := map[string]int{"x": 0}
	assert.Equal(t, "", safeIndex(row, idx, "missing"))
}

func TestParseIntOrNil_EdgeCases(t *testing.T) {
	// Empty string.
	assert.Nil(t, parseIntOrNil(""))

	// "null" sentinel.
	assert.Nil(t, parseIntOrNil("null"))

	// Census missing value sentinel.
	assert.Nil(t, parseIntOrNil("-666666666"))

	// Non-numeric.
	assert.Nil(t, parseIntOrNil("abc"))

	// Valid with whitespace.
	v := parseIntOrNil("  42  ")
	require.NotNil(t, v)
	assert.Equal(t, 42, *v)
}

func TestParseFloatOrNil_EdgeCases(t *testing.T) {
	// Empty string.
	assert.Nil(t, parseFloatOrNil(""))

	// "null" sentinel.
	assert.Nil(t, parseFloatOrNil("null"))

	// Census missing value sentinels.
	assert.Nil(t, parseFloatOrNil("-666666666"))
	assert.Nil(t, parseFloatOrNil("-666666666.0"))

	// Non-numeric.
	assert.Nil(t, parseFloatOrNil("abc"))

	// Valid with whitespace.
	v := parseFloatOrNil("  3.14  ")
	require.NotNil(t, v)
	assert.InDelta(t, 3.14, *v, 0.01)
}

// ---------- Helpers ----------

// sampleDemoBatch returns a minimal batch for demoUpsert testing.
func sampleDemoBatch() [][]any {
	pop := 5000
	inc := 75000.0
	age := 35.2
	housing := 2000
	return [][]any{
		{"48201950100", "tract", 2023, &pop, &inc, &age, &housing,
			"SRID=4326;MULTIPOLYGON(((-95.4 29.7,-95.3 29.7,-95.3 29.8,-95.4 29.7)))",
			"census", "48201950100", []byte("{}")},
	}
}

// expectDemoUpsert sets up pgxmock expectations for a single demoUpsert call.
func expectDemoUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_demographics"}, demoCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
