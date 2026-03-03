package dataset

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestCensusGeo_Metadata(t *testing.T) {
	ds := &CensusGeo{}
	assert.Equal(t, "census_geo", ds.Name())
	assert.Equal(t, "fed_data.fips_codes", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Quarterly, ds.Cadence())
}

func TestCensusGeo_ShouldRun(t *testing.T) {
	ds := &CensusGeo{}

	// Never synced -> should run.
	now := time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced recently within the quarter -> should not run.
	recent := time.Date(2024, time.May, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &recent))

	// Synced last quarter -> should run.
	lastQ := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastQ))
}

func TestCensusGeo_ParseStateGazetteer(t *testing.T) {
	data, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)

	ds := &CensusGeo{}
	rows, stateMap, err := ds.parseStates(strings.NewReader(string(data)))
	require.NoError(t, err)

	assert.Len(t, rows, 3)
	assert.Len(t, stateMap, 3)

	// Verify DC row.
	dc := rows[0]
	assert.Equal(t, "11", dc[0])                      // fips_state
	assert.Equal(t, "000", dc[1])                     // fips_county (sentinel)
	assert.Equal(t, "District of Columbia", dc[2])    // state_name
	assert.Nil(t, dc[3])                              // county_name
	assert.Equal(t, "DC", dc[4])                      // state_abbr
	assert.Nil(t, dc[5])                              // ansi_code (not in state file)
	assert.Equal(t, int64(158364992), dc[6])          // aland
	assert.Equal(t, int64(18633403), dc[7])           // awater
	assert.InDelta(t, 61.148, dc[8], 0.001)           // aland_sqmi
	assert.InDelta(t, 7.195, dc[9], 0.001)            // awater_sqmi
	assert.InDelta(t, 38.9041031, dc[10], 0.0000001)  // intptlat
	assert.InDelta(t, -77.0172290, dc[11], 0.0000001) // intptlong

	// Verify state map.
	assert.Equal(t, "District of Columbia", stateMap["11"])
	assert.Equal(t, "Virginia", stateMap["51"])
	assert.Equal(t, "Maryland", stateMap["24"])
}

func TestCensusGeo_ParseCountyGazetteer(t *testing.T) {
	data, err := os.ReadFile("testdata/gaz_counties_sample.txt")
	require.NoError(t, err)

	stateMap := map[string]string{
		"11": "District of Columbia",
		"51": "Virginia",
		"24": "Maryland",
	}

	ds := &CensusGeo{}
	rows, err := ds.parseCounties(strings.NewReader(string(data)), stateMap)
	require.NoError(t, err)

	assert.Len(t, rows, 5)

	// Verify DC county row (DC is both state and county).
	dc := rows[0]
	assert.Equal(t, "11", dc[0])                   // fips_state
	assert.Equal(t, "001", dc[1])                  // fips_county
	assert.Equal(t, "District of Columbia", dc[2]) // state_name (from stateMap)
	assert.Equal(t, "District of Columbia", dc[3]) // county_name
	assert.Equal(t, "DC", dc[4])                   // state_abbr
	assert.Equal(t, "1702382", dc[5])              // ansi_code

	// Verify Arlington.
	arlington := rows[1]
	assert.Equal(t, "51", arlington[0])                      // fips_state
	assert.Equal(t, "013", arlington[1])                     // fips_county
	assert.Equal(t, "Virginia", arlington[2])                // state_name
	assert.Equal(t, "Arlington County", arlington[3])        // county_name
	assert.Equal(t, "VA", arlington[4])                      // state_abbr
	assert.Equal(t, "1480099", arlington[5])                 // ansi_code
	assert.Equal(t, int64(67336029), arlington[6])           // aland
	assert.Equal(t, int64(636023), arlington[7])             // awater
	assert.InDelta(t, 25.998, arlington[8], 0.001)           // aland_sqmi
	assert.InDelta(t, 0.246, arlington[9], 0.001)            // awater_sqmi
	assert.InDelta(t, 38.8783955, arlington[10], 0.0000001)  // intptlat
	assert.InDelta(t, -77.1005608, arlington[11], 0.0000001) // intptlong

	// Verify Montgomery County.
	montgomery := rows[3]
	assert.Equal(t, "24", montgomery[0])                // fips_state
	assert.Equal(t, "031", montgomery[1])               // fips_county
	assert.Equal(t, "Maryland", montgomery[2])          // state_name
	assert.Equal(t, "Montgomery County", montgomery[3]) // county_name
	assert.Equal(t, "MD", montgomery[4])                // state_abbr
}

func TestCensusGeo_UpsertConfig(t *testing.T) {
	cfg := censusGeoUpsertCfg()
	assert.Equal(t, "fed_data.fips_codes", cfg.Table)
	assert.Equal(t, []string{"fips_state", "fips_county"}, cfg.ConflictKeys)
	assert.Equal(t, censusGeoColumns, cfg.Columns)
	assert.Len(t, cfg.Columns, 13)
}

func TestCensusGeo_ParseStateGazetteer_EmptyFile(t *testing.T) {
	ds := &CensusGeo{}
	_, _, err := ds.parseStates(strings.NewReader(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty state file")
}

func TestCensusGeo_ParseCountyGazetteer_EmptyFile(t *testing.T) {
	ds := &CensusGeo{}
	_, err := ds.parseCounties(strings.NewReader(""), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty county file")
}

func TestCensusGeo_ParseStateGazetteer_SkipsEmptyGEOID(t *testing.T) {
	input := "USPS|GEOID|NAME|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n" +
		"DC||District of Columbia|158364992|18633403|61.148|7.195|38.904|-77.017\n" +
		"VA|51|Virginia|102257717110|8528531774|39490.086|3293.070|37.522|-78.668\n"

	ds := &CensusGeo{}
	rows, stateMap, err := ds.parseStates(strings.NewReader(input))
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Len(t, stateMap, 1)
	assert.Equal(t, "Virginia", stateMap["51"])
}

func TestCensusGeo_ParseCountyGazetteer_SkipsShortGEOID(t *testing.T) {
	input := "USPS|GEOID|ANSICODE|NAME|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n" +
		"DC|11|1702382|District of Columbia|158364992|18633403|61.148|7.195|38.904|-77.017\n" +
		"VA|51013|1480099|Arlington County|67336029|636023|25.998|0.246|38.878|-77.100\n"

	ds := &CensusGeo{}
	rows, err := ds.parseCounties(strings.NewReader(input), map[string]string{"51": "Virginia"})
	require.NoError(t, err)
	assert.Len(t, rows, 1) // Only Arlington, DC's GEOID "11" is too short
	assert.Equal(t, "013", rows[0][1])
}

func TestCensusGeo_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	stateContent, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)
	countyContent, err := os.ReadFile("testdata/gaz_counties_sample.txt")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	year := time.Now().Year()

	// Mock detectYear: Download succeeds for the state URL.
	stateURL := stateGazURL(year)
	f.EXPECT().Download(mock.Anything, stateURL).
		Return(newNopReadCloser("ok"), nil).Once()

	// Mock state ZIP download.
	f.EXPECT().DownloadToFile(mock.Anything, stateURL, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_state_national.txt", year), string(stateContent))).
		Once()

	// Mock county ZIP download.
	countyURL := countyGazURL(year)
	f.EXPECT().DownloadToFile(mock.Anything, countyURL, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_counties_national.txt", year), string(countyContent))).
		Once()

	// Expect one BulkUpsert for all 8 rows (3 states + 5 counties).
	expectBulkUpsert(pool, "fed_data.fips_codes", censusGeoColumns, 8)

	ds := &CensusGeo{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(8), result.RowsSynced)
	assert.Equal(t, 3, result.Metadata["states"])
	assert.Equal(t, 5, result.Metadata["counties"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCensusGeo_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// detectYear: Download fails with 404 → falls back to year-1.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, assert.AnError).Once()

	// State download fails.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError).Once()

	ds := &CensusGeo{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download state gazetteer")
}

func TestCensusGeo_Sync_UpsertError(t *testing.T) {
	dir := t.TempDir()

	stateContent, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)
	countyContent, err := os.ReadFile("testdata/gaz_counties_sample.txt")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	year := time.Now().Year()

	// detectYear succeeds.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(newNopReadCloser("ok"), nil).Once()

	// State and county downloads succeed.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_state_national.txt", year), string(stateContent))).
		Once()
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_counties_national.txt", year), string(countyContent))).
		Once()

	// BulkUpsert fails.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &CensusGeo{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "census_geo: bulk upsert")
}

func TestCensusGeo_URLs(t *testing.T) {
	assert.Equal(t,
		"https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_state_national.zip",
		stateGazURL(2024),
	)
	assert.Equal(t,
		"https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip",
		countyGazURL(2024),
	)
}

func TestCensusGeo_DetectDelimiter(t *testing.T) {
	assert.Equal(t, "|", detectDelimiter("USPS|GEOID|NAME"))
	assert.Equal(t, "\t", detectDelimiter("USPS\tGEOID\tNAME"))
}

func TestCensusGeo_ParseHelpers(t *testing.T) {
	// parseInt64OrNil
	assert.Equal(t, int64(42), parseInt64OrNil("42"))
	assert.Equal(t, int64(0), parseInt64OrNil("0"))
	assert.Nil(t, parseInt64OrNil(""))
	assert.Nil(t, parseInt64OrNil("abc"))

	// parseFloat64OrNil
	assert.InDelta(t, 3.14, parseFloat64OrNil("3.14"), 0.001)
	assert.InDelta(t, float64(0), parseFloat64OrNil("0"), 0.001)
	assert.Nil(t, parseFloat64OrNil(""))
	assert.Nil(t, parseFloat64OrNil("xyz"))

	// nilIfEmpty
	assert.Nil(t, nilIfEmpty(""))
	assert.Equal(t, "hello", nilIfEmpty("hello"))
}

func TestCensusGeo_ParseStates_TabDelimited(t *testing.T) {
	input := "USPS\tGEOID\tNAME\tALAND\tAWATER\tALAND_SQMI\tAWATER_SQMI\tINTPTLAT\tINTPTLONG\n" +
		"DC\t11\tDistrict of Columbia\t158364992\t18633403\t61.148\t7.195\t38.904\t-77.017\n"

	ds := &CensusGeo{}
	rows, stateMap, err := ds.parseStates(strings.NewReader(input))
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "District of Columbia", stateMap["11"])
}

func TestCensusGeo_Sync_CountyDownloadError(t *testing.T) {
	dir := t.TempDir()

	stateContent, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	year := time.Now().Year()

	// detectYear succeeds.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(newNopReadCloser("ok"), nil).Once()

	// State download succeeds.
	f.EXPECT().DownloadToFile(mock.Anything, stateGazURL(year), mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_state_national.txt", year), string(stateContent))).
		Once()

	// County download fails.
	f.EXPECT().DownloadToFile(mock.Anything, countyGazURL(year), mock.Anything).
		Return(int64(0), assert.AnError).Once()

	ds := &CensusGeo{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download county gazetteer")
}

func TestCensusGeo_Sync_DetectYearFallback404(t *testing.T) {
	dir := t.TempDir()

	stateContent, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)
	countyContent, err := os.ReadFile("testdata/gaz_counties_sample.txt")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	year := time.Now().Year()
	fallbackYear := year - 1

	// detectYear fails with 404 → falls back to year-1.
	f.EXPECT().Download(mock.Anything, stateGazURL(year)).
		Return(nil, fmt.Errorf("HTTP 404 not found")).Once()

	// Downloads use fallback year.
	f.EXPECT().DownloadToFile(mock.Anything, stateGazURL(fallbackYear), mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_state_national.txt", fallbackYear), string(stateContent))).
		Once()
	f.EXPECT().DownloadToFile(mock.Anything, countyGazURL(fallbackYear), mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_counties_national.txt", fallbackYear), string(countyContent))).
		Once()

	expectBulkUpsert(pool, "fed_data.fips_codes", censusGeoColumns, 8)

	ds := &CensusGeo{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(8), result.RowsSynced)
	assert.Equal(t, fallbackYear, result.Metadata["year"])
}

func TestCensusGeo_Sync_StateExtractError(t *testing.T) {
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// detectYear succeeds.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(newNopReadCloser("ok"), nil).Once()

	// State download writes a non-ZIP file → ExtractZIP will fail.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return 7, os.WriteFile(path, []byte("not-zip"), 0644)
		}).Once()

	ds := &CensusGeo{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extract state ZIP")
}

func TestCensusGeo_Sync_CountyExtractError(t *testing.T) {
	dir := t.TempDir()

	stateContent, err := os.ReadFile("testdata/gaz_state_sample.txt")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	year := time.Now().Year()

	// detectYear succeeds.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(newNopReadCloser("ok"), nil).Once()

	// State download succeeds.
	f.EXPECT().DownloadToFile(mock.Anything, stateGazURL(year), mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, fmt.Sprintf("%d_Gaz_state_national.txt", year), string(stateContent))).
		Once()

	// County download writes a non-ZIP file → ExtractZIP will fail.
	f.EXPECT().DownloadToFile(mock.Anything, countyGazURL(year), mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return 7, os.WriteFile(path, []byte("not-zip"), 0644)
		}).Once()

	ds := &CensusGeo{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extract county ZIP")
}

func TestCensusGeo_ParseCounties_EmptyANSI(t *testing.T) {
	// County with empty ANSI code should produce nil.
	input := "USPS|GEOID|ANSICODE|NAME|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n" +
		"VA|51013||Arlington County|67336029|636023|25.998|0.246|38.878|-77.100\n"

	ds := &CensusGeo{}
	rows, err := ds.parseCounties(strings.NewReader(input), map[string]string{"51": "Virginia"})
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Nil(t, rows[0][5]) // ansi_code should be nil
}

// newNopReadCloser returns an io.ReadCloser that reads from the given string.
func newNopReadCloser(s string) nopReadCloser {
	return nopReadCloser{strings.NewReader(s)}
}

type nopReadCloser struct {
	*strings.Reader
}

func (nopReadCloser) Close() error { return nil }
