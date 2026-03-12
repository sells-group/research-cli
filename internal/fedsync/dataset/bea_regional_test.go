package dataset

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

const beaTestCSV = `GeoFips,GeoName,LineCode,Description,Unit,2021,2022,2023
"48000","Texas",1,"All industry total","Thousands of dollars",2000000,2100000,2200000
"48453","Travis, TX",1,"All industry total","Thousands of dollars",100000,110000,120000
"48113","Dallas, TX",1,"All industry total","Thousands of dollars",200000,210000,(NA)
"US0","United States",1,"All industry total","Thousands of dollars",25000000,26000000,27000000
`

func TestBEARegional_Metadata(t *testing.T) {
	d := &BEARegional{}
	assert.Equal(t, "bea_regional", d.Name())
	assert.Equal(t, "fed_data.bea_regional", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Annual, d.Cadence())
}

func TestBEARegional_ShouldRun(t *testing.T) {
	d := &BEARegional{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced after release", func(t *testing.T) {
		// Now is December 2025, last sync was November 15 2025 (after November 1 release).
		now := time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, time.November, 15, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced before release", func(t *testing.T) {
		// Now is December 2025, last sync was October 2025 (before November 1 release).
		now := time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, time.October, 1, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})

	t.Run("before release date", func(t *testing.T) {
		// Now is October 2025, before November release — should not run.
		now := time.Date(2025, time.October, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2024, time.December, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})
}

func TestBEARegional_Sync(t *testing.T) {
	dir := t.TempDir()

	// Build a ZIP containing the test CSV for each of the 3 BEA tables.
	zipPath := createTestZip(t, dir, "bea_test.zip", "CAGDP1__ALL_AREAS.csv", beaTestCSV)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Mock DownloadToFile for all 3 table ZIPs (CAGDP1, CAINC1, CAINC4).
	mockDownloadToFile(f, zipPath).Times(3)

	// Each table produces 8 value rows:
	//   row1 (48000, 5-digit? no — "48000" is 5 chars) => 3 year values
	//   row2 (48453, 5-digit) => 3 year values
	//   row3 (48113, 5-digit) => 2 year values (one is NA)
	//   row4 (US0, 3-char) => skipped
	//   Total = 3 + 3 + 2 = 8 rows per table
	// 3 tables × 8 = 24 total rows.
	// All 8 rows fit in one batch (< 5000), so one BulkUpsert per table.
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 8)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 8)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 8)

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(24), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBEARegional_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("network timeout")).Once()

	d := &BEARegional{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestBEARegional_NAValues(t *testing.T) {
	dir := t.TempDir()

	// CSV where all data values are (NA) — should produce 0 rows.
	naCSV := `GeoFips,GeoName,LineCode,Description,Unit,2021,2022,2023
"48000","Texas",1,"All industry total","Thousands of dollars",(NA),(NA),(NA)
"48453","Travis, TX",1,"All industry total","Thousands of dollars",(D),(L),(T)
`
	zipPath := createTestZip(t, dir, "bea_na.zip", "CAGDP1__ALL_AREAS.csv", naCSV)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Times(3)

	// No BulkUpsert expected since all values are suppressed markers.

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBEARegional_ExtractZIPError(t *testing.T) {
	// Tests the error path when the ZIP extraction fails (corrupt ZIP).
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Write a non-ZIP file that will fail extraction.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			content := "not a zip file"
			return int64(len(content)), os.WriteFile(destPath, []byte(content), 0644)
		}).Once()

	d := &BEARegional{baseURL: "https://example.com/test.zip"}
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestBEARegional_NoCSVInZIP(t *testing.T) {
	// Tests the error path when the ZIP contains no CSV file.
	dir := t.TempDir()

	// Create a ZIP with only a .txt file (no .csv).
	zipPath := createTestZip(t, dir, "bea_no_csv.zip", "readme.txt", "this is not a CSV")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Once()

	d := &BEARegional{baseURL: "file://" + zipPath}
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV found")
}

func TestBEARegional_UpsertError(t *testing.T) {
	// Tests that a BulkUpsert failure is propagated.
	dir := t.TempDir()

	zipPath := createTestZip(t, dir, "bea_upsert.zip", "CAGDP1__ALL_AREAS.csv", beaTestCSV)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Once()

	// Make BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(errors.New("db connection lost"))

	d := &BEARegional{baseURL: "file://" + zipPath}
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestBEARegional_InvalidLineCode(t *testing.T) {
	// Tests that rows with non-integer LineCode are skipped.
	dir := t.TempDir()

	csv := `GeoFips,GeoName,LineCode,Description,Unit,2021
"48000","Texas",ABC,"All industry total","Thousands of dollars",2000000
"48453","Travis, TX",1,"All industry total","Thousands of dollars",100000
`
	zipPath := createTestZip(t, dir, "bea_badline.zip", "CAGDP1__ALL_AREAS.csv", csv)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Times(3)

	// Only 1 valid row per table × 3 tables = 3.
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBEARegional_InvalidValueSkipped(t *testing.T) {
	// Tests that rows with non-numeric value strings (not matching known markers)
	// are silently skipped.
	dir := t.TempDir()

	csv := `GeoFips,GeoName,LineCode,Description,Unit,2021,2022
"48000","Texas",1,"All industry total","Thousands of dollars",abc,2000000
"48453","Travis, TX",1,"All industry total","Thousands of dollars",100000,xyz
`
	zipPath := createTestZip(t, dir, "bea_badval.zip", "CAGDP1__ALL_AREAS.csv", csv)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Times(3)

	// Each table: row1 has 1 valid value (2022), row2 has 1 valid value (2021) → 2 per table.
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(6), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBEARegional_ProdURL_Format(t *testing.T) {
	// Tests that the production URL is correctly constructed when baseURL is empty.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	var attemptedURLs []string
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, _ string) (int64, error) {
			attemptedURLs = append(attemptedURLs, url)
			return 0, errors.New("not found")
		}).Once()

	d := &BEARegional{} // no baseURL → production path
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)

	// Verify the first URL tried is for CAGDP1.
	require.Len(t, attemptedURLs, 1)
	assert.Equal(t, "https://apps.bea.gov/regional/zip/CAGDP1.zip", attemptedURLs[0])
}

func TestBEARegional_OpenCSVError(t *testing.T) {
	// Tests the error path when the CSV cannot be opened after extraction.
	// We simulate this by creating a ZIP where the CSV entry name has directory
	// components that won't be created properly.
	dir := t.TempDir()

	// Create a ZIP with a CSV file, then remove it after extraction.
	zipPath := createTestZip(t, dir, "bea_open.zip", "data.csv", beaTestCSV)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			data, rErr := os.ReadFile(zipPath)
			if rErr != nil {
				return 0, rErr
			}
			if wErr := os.WriteFile(destPath, data, 0644); wErr != nil {
				return 0, wErr
			}
			// After writing the ZIP, pre-create the extract dir and put a directory
			// where the CSV file should be, causing os.Open to fail.
			extractDir := destPath[:len(destPath)-4] + "_extract"
			csvDir := filepath.Join(extractDir, "data.csv")
			_ = os.MkdirAll(csvDir, 0755) // Make a dir where the file should be.
			return int64(len(data)), nil
		}).Once()

	d := &BEARegional{baseURL: "file://" + zipPath}
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	// Could be "extract" or "open" depending on whether ExtractZIP fails when
	// there's a dir collision.
}

func TestBEARegional_EmptyCSVHeader(t *testing.T) {
	// Tests the error path when the CSV is empty (no header row).
	dir := t.TempDir()

	zipPath := createTestZip(t, dir, "bea_empty.zip", "CAGDP1__ALL_AREAS.csv", "")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Once()

	d := &BEARegional{baseURL: "file://" + zipPath}
	_, err = d.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read")
	assert.Contains(t, err.Error(), "header")
}

func TestBEARegional_FIPSFiltering(t *testing.T) {
	// Tests that non-county/non-state FIPS codes are filtered out.
	dir := t.TempDir()

	csv := `GeoFips,GeoName,LineCode,Description,Unit,2021
"4800000","Texas (7-digit)  ",1,"Total","USD",1000
"480","Region (3-digit)",1,"Total","USD",2000
"48","Texas (2-digit, state)",1,"Total","USD",3000
"48453","Travis (5-digit, county)",1,"Total","USD",4000
"4","Too short",1,"Total","USD",5000
`
	zipPath := createTestZip(t, dir, "bea_fips.zip", "CAGDP1__ALL_AREAS.csv", csv)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Times(3)

	// Only 2-digit (48) and 5-digit (48453) FIPS pass → 2 rows per table × 3 = 6.
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 2)

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(6), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBEARegional_CommaInValue(t *testing.T) {
	// Tests that commas in numeric values are stripped before parsing.
	dir := t.TempDir()

	csv := `GeoFips,GeoName,LineCode,Description,Unit,2021
"48000","Texas",1,"All industry total","Thousands of dollars","2,000,000"
`
	zipPath := createTestZip(t, dir, "bea_comma.zip", "CAGDP1__ALL_AREAS.csv", csv)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath).Times(3)

	// 1 row per table × 3 = 3.
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)
	expectBulkUpsert(pool, "fed_data.bea_regional", beaCols, 1)

	d := &BEARegional{baseURL: "file://" + zipPath}
	result, err := d.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}
