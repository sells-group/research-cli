package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jonas-p/go-shp"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

func TestTIGERBoundaries_Metadata(t *testing.T) {
	s := &TIGERBoundaries{}
	assert.Equal(t, "tiger_boundaries", s.Name())
	assert.Equal(t, "geo.counties", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestTIGERBoundaries_ShouldRun(t *testing.T) {
	s := &TIGERBoundaries{}

	// Never synced → should run.
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	// Use a date after October to test annual logic properly.
	nowNov := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	// Synced after October 1 of this year → should not run.
	recent := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(nowNov, &recent))

	// Synced before October of this year → should run.
	stale := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(nowNov, &stale))
}

func TestClassifyRoad(t *testing.T) {
	tests := []struct {
		mtfcc string
		want  string
	}{
		{"S1100", "interstate"},
		{"S1200", "us_highway"},
		{"S1300", "state_highway"},
		{"S1400", "local"},
		{"", "local"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, classifyRoad(tt.mtfcc), "mtfcc=%s", tt.mtfcc)
	}
}

func TestNewCountyRow(t *testing.T) {
	raw := []any{
		"48",               // statefp
		"453",              // countyfp
		"48453",            // geoid
		"Travis",           // name
		"Travis County",    // namelsad
		"06",               // lsad
		"G4020",            // mtfcc
		"A",                // funcstat
		"2546422",          // aland
		"18993",            // awater
		"30.3340",          // intptlat
		"-97.7715",         // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newCountyRow(raw)
	assert.Equal(t, "48453", row[0])          // geoid
	assert.Equal(t, "48", row[1])             // state_fips
	assert.Equal(t, "453", row[2])            // county_fips
	assert.Equal(t, "Travis", row[3])         // name
	assert.Equal(t, "06", row[4])             // lsad
	assert.Equal(t, raw[12], row[5])          // geom
	assert.InDelta(t, 30.334, row[6], 0.001)  // latitude
	assert.InDelta(t, -97.772, row[7], 0.001) // longitude
	assert.Equal(t, tigerGeoSource, row[8])
	assert.Equal(t, "tiger/48453", row[9])
}

func TestNewPlaceRow(t *testing.T) {
	raw := []any{
		"48",               // statefp
		"05000",            // placefp
		"4805000",          // geoid
		"Austin",           // name
		"Austin city",      // namelsad
		"25",               // lsad
		"C1",               // classfp
		"G4110",            // mtfcc
		"A",                // funcstat
		"771926",           // aland
		"20495",            // awater
		"30.2672",          // intptlat
		"-97.7431",         // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newPlaceRow(raw)
	assert.Equal(t, "4805000", row[0]) // geoid
	assert.Equal(t, "48", row[1])      // state_fips
	assert.Equal(t, "05000", row[2])   // place_fips
	assert.Equal(t, "Austin", row[3])  // name
	assert.Equal(t, "25", row[4])      // lsad
	assert.Equal(t, "C1", row[5])      // class_fips
	assert.Equal(t, raw[13], row[6])   // geom
}

func TestNewZCTARow(t *testing.T) {
	raw := []any{
		"78701",            // zcta5ce20
		"7878701",          // geoid20
		"B5",               // classfp20
		"G6350",            // mtfcc20
		"S",                // funcstat20
		"2000000",          // aland20
		"50000",            // awater20
		"30.2672",          // intptlat20
		"-97.7431",         // intptlon20
		[]byte{0x01, 0x02}, // wkb
	}

	row := newZCTARow(raw)
	assert.Equal(t, "78701", row[0]) // zcta5
	assert.Equal(t, "78", row[1])    // state_fips (first 2 of geoid20)
	require.NotNil(t, row[2])
	assert.Equal(t, int64(2000000), *row[2].(*int64)) // aland
}

func TestNewCBSARow(t *testing.T) {
	raw := []any{
		"12420",                            // cbsafp
		"Austin-Round Rock-Georgetown, TX", // name
		"Austin-Round Rock-Georgetown, TX Metro Area", // namelsad
		"M1",               // lsad
		"G3110",            // mtfcc
		"11000000",         // aland
		"500000",           // awater
		"30.5",             // intptlat
		"-97.8",            // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newCBSARow(raw)
	assert.Equal(t, "12420", row[0])                            // cbsa_code
	assert.Equal(t, "Austin-Round Rock-Georgetown, TX", row[1]) // name
	assert.Equal(t, "M1", row[2])                               // lsad
}

func TestNewCensusTractRow(t *testing.T) {
	raw := []any{
		"48",               // statefp
		"453",              // countyfp
		"002100",           // tractce
		"48453002100",      // geoid
		"21",               // name
		"Census Tract 21",  // namelsad
		"G5020",            // mtfcc
		"S",                // funcstat
		"1234567",          // aland
		"12345",            // awater
		"30.29",            // intptlat
		"-97.74",           // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newCensusTractRow(raw)
	assert.Equal(t, "48453002100", row[0]) // geoid
	assert.Equal(t, "48", row[1])          // state_fips
	assert.Equal(t, "453", row[2])         // county_fips
	assert.Equal(t, "002100", row[3])      // tract_ce
	assert.Equal(t, "21", row[4])          // name
}

func TestNewCongressionalDistrictRow(t *testing.T) {
	raw := []any{
		"48",                        // statefp
		"21",                        // cd118fp
		"4821",                      // geoid
		"Congressional District 21", // namelsad
		"C2",                        // lsad
		"G5210",                     // mtfcc
		"N",                         // funcstat
		"55000000",                  // aland
		"1000000",                   // awater
		"30.0",                      // intptlat
		"-98.0",                     // intptlon
		[]byte{0x01, 0x02},          // wkb
	}

	row := newCongressionalDistrictRow(raw)
	assert.Equal(t, "4821", row[0])                      // geoid
	assert.Equal(t, "48", row[1])                        // state_fips
	assert.Equal(t, "21", row[2])                        // district
	assert.Equal(t, "118", row[3])                       // congress
	assert.Equal(t, "Congressional District 21", row[4]) // name
	assert.Equal(t, "C2", row[5])                        // lsad
}

func TestTIGERBoundaries_Sync(t *testing.T) {
	// Create a single county shapefile for testing.
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, countyProduct.Columns, 3)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 6 boundary types: counties(3), places(3), zcta(3), cbsa(3), tracts(3×51 states), congressional(3).
	// With test override base URL, per-state tracts will all get the same file.
	// Each boundary type gets one upsert with 3 rows.
	// counties
	expectBoundaryUpsert(mock, "geo_counties", countyCols, 3)
	// places
	expectBoundaryUpsert(mock, "geo_places", placeCols, 3)
	// zcta
	expectBoundaryUpsert(mock, "geo_zcta", zctaCols, 3)
	// cbsa
	expectBoundaryUpsert(mock, "geo_cbsa", cbsaCols, 3)
	// census_tracts (per-state: 51 states × 3 rows = 51 upserts)
	for range 51 {
		expectBoundaryUpsert(mock, "geo_census_tracts", censusTractCols, 3)
	}
	// congressional_districts
	expectBoundaryUpsert(mock, "geo_congressional_districts", congressionalDistrictCols, 3)

	s := &TIGERBoundaries{downloadBaseURL: srv.URL, year: 2024}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	// 5 national × 3 + 51 states × 3 = 15 + 153 = 168
	assert.Equal(t, int64(168), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTIGERBoundaries_StateDownloadError(t *testing.T) {
	// Create a valid shapefile for national downloads.
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, countyProduct.Columns, 2)

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// Serve valid data for all requests except TRACT downloads.
		// The TRACT pattern contains the FIPS in the URL: /TRACT/tl_2024_{fips}_tract.zip
		if filepath.Base(r.URL.Path) != "" && len(filepath.Base(r.URL.Path)) > 8 {
			// Check for per-state tract URLs by looking for state FIPS pattern.
			base := filepath.Base(r.URL.Path)
			if len(base) > 14 && base[:8] == "tl_2024_" && base[10:] == "_tract.zip" {
				http.Error(w, "not found", 404)
				return
			}
		}
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// National boundary types get upserted, per-state tracts all fail → skipped.
	expectBoundaryUpsert(mock, "geo_counties", countyCols, 2)
	expectBoundaryUpsert(mock, "geo_places", placeCols, 2)
	expectBoundaryUpsert(mock, "geo_zcta", zctaCols, 2)
	expectBoundaryUpsert(mock, "geo_cbsa", cbsaCols, 2)
	// No tract upserts (all states fail)
	expectBoundaryUpsert(mock, "geo_congressional_districts", congressionalDistrictCols, 2)

	s := &TIGERBoundaries{downloadBaseURL: srv.URL, year: 2024}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	// 5 national × 2 = 10, no tracts.
	assert.Equal(t, int64(10), result.RowsSynced)
}

func TestTIGERBoundaries_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// National file download fails immediately.
	s := &TIGERBoundaries{downloadBaseURL: "http://127.0.0.1:1", year: 2024}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

func TestTIGERBoundaries_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &TIGERBoundaries{downloadBaseURL: "http://127.0.0.1:1", year: 2024}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

func TestTIGERBoundaries_UpsertError(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, countyProduct.Columns, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// BulkUpsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &TIGERBoundaries{downloadBaseURL: srv.URL, year: 2024}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestTIGERBoundaries_EffectiveYear_Default(t *testing.T) {
	s := &TIGERBoundaries{} // year = 0 → uses tigerYear
	assert.Equal(t, tigerYear, s.effectiveYear())
}

func TestStrVal_NonString(t *testing.T) {
	raw := []any{42}
	assert.Equal(t, "42", strVal(raw, 0))
}

func TestStrVal_NilAndOutOfBounds(t *testing.T) {
	raw := []any{nil}
	assert.Equal(t, "", strVal(raw, 0))
	assert.Equal(t, "", strVal(raw, 5)) // out of bounds
}

func TestParseInt64Val_InvalidString(t *testing.T) {
	raw := []any{"notanumber"}
	assert.Nil(t, parseInt64Val(raw, 0))
}

func TestTigerURL_Default(t *testing.T) {
	url := tigerURL("", 2024, "COUNTY/tl_2024_us_county.zip")
	assert.Contains(t, url, "census.gov")
	assert.Contains(t, url, "TIGER2024")
	assert.Contains(t, url, "tl_2024_us_county.zip")
}

func TestTIGERBoundaries_EffectiveYear_Override(t *testing.T) {
	s := &TIGERBoundaries{year: 2023}
	assert.Equal(t, 2023, s.effectiveYear())
}

func TestTIGERBoundaries_BuildURL_National(t *testing.T) {
	s := &TIGERBoundaries{year: 2024}
	def := countyDef()
	url := s.buildURL(def, 2024, "")
	assert.Contains(t, url, "COUNTY/tl_2024_us_county.zip")
}

func TestTIGERBoundaries_BuildURL_PerState(t *testing.T) {
	s := &TIGERBoundaries{year: 2024}
	def := censusTractDef()
	url := s.buildURL(def, 2024, "48")
	assert.Contains(t, url, "TRACT/tl_2024_48_tract.zip")
}

func TestFilterToProductColumns_MissingColumn(t *testing.T) {
	// Product expects a column not in the result — should get nil/zero value.
	result := &tiger.ParseResult{
		Columns: []string{"name", "geom"},
		Rows:    [][]any{{"test", []byte{0x01}}},
	}
	p := tiger.Product{
		Name:     "TEST",
		Columns:  []string{"name", "missing_col"},
		GeomType: "",
	}
	filtered := filterToProductColumns(result, p)
	assert.Equal(t, "test", filtered.Rows[0][0])
	assert.Nil(t, filtered.Rows[0][1]) // missing_col
}

// ---------- Helpers ----------

// expectBoundaryUpsert sets up pgxmock expectations for a single BulkUpsert call.
func expectBoundaryUpsert(mock pgxmock.PgxPoolIface, tempTable string, cols []string, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_" + tempTable}, cols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

// createTestBoundaryShapefile creates a polygon shapefile with n features and the
// given column names, zips it, and returns the path to the ZIP file.
func createTestBoundaryShapefile(t *testing.T, shpType shp.ShapeType, columns []string, n int) string {
	t.Helper()
	dir := t.TempDir()
	shpPath := filepath.Join(dir, "test.shp")

	fields := make([]shp.Field, len(columns))
	for i, col := range columns {
		fields[i] = shp.StringField(col, 50)
	}

	w, err := shp.Create(shpPath, shpType)
	require.NoError(t, err)
	require.NoError(t, w.SetFields(fields))

	for i := 0; i < n; i++ {
		points := []shp.Point{
			{X: -98.0 + float64(i)*0.1, Y: 30.0},
			{X: -98.0 + float64(i)*0.1, Y: 31.0},
			{X: -97.0 + float64(i)*0.1, Y: 31.0},
			{X: -97.0 + float64(i)*0.1, Y: 30.0},
			{X: -98.0 + float64(i)*0.1, Y: 30.0},
		}
		poly := &shp.Polygon{
			Box:       shp.BBoxFromPoints(points),
			NumParts:  1,
			NumPoints: int32(len(points)),
			Parts:     []int32{0},
			Points:    points,
		}
		idx := w.Write(poly)
		for j := range columns {
			require.NoError(t, w.WriteAttribute(int(idx), j, testBoundaryValue(columns[j], i)))
		}
	}
	w.Close()
	fixShpDBF(t, dir, "test")

	zipPath := filepath.Join(dir, "test.zip")
	zipShapefile(t, zipPath, dir, "test")
	return zipPath
}

// testBoundaryValue returns a plausible test value for a given column name.
func testBoundaryValue(col string, idx int) string {
	switch col {
	case "statefp":
		return "48"
	case "countyfp":
		fips := 100 + idx
		return padLeft(fips, 3)
	case "geoid", "geoid20":
		return "48" + padLeft(100+idx, 3)
	case "name", "namelsad":
		return "Test Name"
	case "lsad":
		return "06"
	case "classfp", "classfp20":
		return "C1"
	case "mtfcc", "mtfcc20":
		return "G4020"
	case "funcstat", "funcstat20":
		return "A"
	case "aland", "aland20":
		return "1000000"
	case "awater", "awater20":
		return "50000"
	case "intptlat", "intptlat20":
		return "30.2672"
	case "intptlon", "intptlon20":
		return "-97.7431"
	case "placefp":
		return "05000"
	case "zcta5ce20":
		return "78" + padLeft(700+idx, 3)
	case "cbsafp":
		return "12420"
	case "tractce":
		return "002100"
	case "cd118fp":
		return "21"
	default:
		return "test"
	}
}

// createCorruptShapefileZIP creates a ZIP archive containing a corrupt .shp file
// that will cause tiger.ParseShapefile to fail.
func createCorruptShapefileZIP(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Write a file with .shp extension but invalid content.
	shpPath := filepath.Join(dir, "test.shp")
	require.NoError(t, os.WriteFile(shpPath, []byte("this is not a valid shapefile"), 0o644))

	zipPath := filepath.Join(dir, "corrupt.zip")
	zipShapefile(t, zipPath, dir, "test")
	return zipPath
}

// padLeft zero-pads an integer to the given width.
func padLeft(n, width int) string {
	s := ""
	for v := n; v > 0; v /= 10 {
		s = string(rune('0'+v%10)) + s
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
}
