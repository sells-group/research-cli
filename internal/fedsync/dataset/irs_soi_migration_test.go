package dataset

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestIRSSOIMigration_Metadata(t *testing.T) {
	ds := &IRSSOIMigration{}
	assert.Equal(t, "irs_soi_migration", ds.Name())
	assert.Equal(t, "fed_data.irs_soi_migration", ds.Table())
	assert.Equal(t, Phase2, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestIRSSOIMigration_ShouldRun(t *testing.T) {
	ds := &IRSSOIMigration{}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	// nil lastSync → should run.
	assert.True(t, ds.ShouldRun(now, nil))

	// Recent sync → should not run.
	recent := time.Date(2026, time.July, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &recent))
}

// inflowCSV is the testdata content for inflow direction.
var inflowCSV = mustReadTestdata("irs_soi_inflow.csv")

// outflowCSV is inline test data for outflow direction.
// Row 1: valid county-to-county flow.
// Row 2: county_fips_origin "000" → skipped (summary).
// Row 3: dest state "97" → skipped (non-migrant).
const outflowCSV = `y1_statefips,y1_countyfips,y2_statefips,y2_countyfips,y2_state,y2_countyname,Return_Num,Exmpt_Num,Adjusted_Gross_Income
48,453,06,037,CA,Los Angeles County,300,700,35000
48,000,06,037,CA,Los Angeles County,800,1900,70000
48,453,97,000,--,Non-migrant,5000,12000,500000
`

func mustReadTestdata(name string) string {
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		panic("mustReadTestdata: " + err.Error())
	}
	return string(data)
}

// mockIRSDownloads sets up DownloadToFile mock for both inflow and outflow CSVs.
// It inspects the dest path to decide which CSV content to write.
func mockIRSDownloads(f *fetchermocks.MockFetcher, inflowContent, outflowContent string) {
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			var content string
			if strings.Contains(destPath, "inflow") {
				content = inflowContent
			} else {
				content = outflowContent
			}
			if err := os.WriteFile(destPath, []byte(content), 0o644); err != nil {
				panic("mockIRSDownloads: " + err.Error())
			}
		}).
		Return(int64(1000), nil).
		Times(2)
}

func TestIRSSOIMigration_Sync(t *testing.T) {
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	mockIRSDownloads(f, inflowCSV, outflowCSV)

	// Inflow: 4 rows total, 2 skipped (county "000" and state "96") → 2 rows.
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 2)
	// Outflow: 3 rows total, 2 skipped (county "000" and state "97") → 1 row.
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 1)

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestIRSSOIMigration_SkipSummaryRows(t *testing.T) {
	dir := t.TempDir()

	// All rows are summary/non-migrant rows → 0 rows synced.
	allSkippedCSV := `y1_statefips,y1_countyfips,y2_statefips,y2_countyfips,y2_state,y2_countyname,Return_Num,Exmpt_Num,Adjusted_Gross_Income
48,000,06,037,CA,Los Angeles County,800,1900,70000
06,037,48,000,TX,Travis County,1000,2500,90000
96,001,48,453,TX,Travis County,50,100,5000
97,001,48,453,TX,Travis County,60,120,6000
98,001,48,453,TX,Travis County,70,140,7000
48,453,96,001,--,Non-migrant,5000,12000,500000
48,453,97,001,--,Non-migrant,5000,12000,500000
48,453,98,001,--,Non-migrant,5000,12000,500000
`

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	// Both inflow and outflow get the same all-skipped CSV.
	mockIRSDownloads(f, allSkippedCSV, allSkippedCSV)

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestIRSSOIMigration_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("connection refused"))

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestIRSSOIMigration_ProdURL_FallbackSuccess(t *testing.T) {
	// Tests the production URL year-pair fallback loop (no baseURL).
	// First pair fails for both directions, second pair succeeds.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	y := time.Now().Year()
	// Build expected year pairs.
	pair1 := fmt.Sprintf("%02d%02d", (y-2)%100, (y-1)%100)

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, destPath string) (int64, error) {
			callCount++
			// Fail for first pair (inflow).
			if strings.Contains(url, pair1) {
				return 0, fmt.Errorf("HTTP 404 not found")
			}
			// Succeed for second pair — write the appropriate CSV.
			var content string
			if strings.Contains(destPath, "inflow") {
				content = inflowCSV
			} else {
				content = outflowCSV
			}
			if wErr := os.WriteFile(destPath, []byte(content), 0o644); wErr != nil {
				return 0, wErr
			}
			return int64(len(content)), nil
		}).Times(4) // pair1 fails for inflow, pair2 succeeds for inflow, pair1 fails for outflow, pair2 succeeds for outflow

	// Inflow: 2 valid rows, Outflow: 1 valid row.
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 2)
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 1)

	ds := &IRSSOIMigration{} // no baseURL → production path
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestIRSSOIMigration_ProdURL_AllPairsFail(t *testing.T) {
	// Tests that all 4 year-pair attempts failing returns an error.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("not found")).Times(4) // 4 pairs for inflow direction

	ds := &IRSSOIMigration{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tried pairs")
}

func TestIRSSOIMigration_ProdURL_FirstPairSuccess(t *testing.T) {
	// Tests that the production URL loop succeeds on the first pair.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	y := time.Now().Year()
	pair1 := fmt.Sprintf("%02d%02d", (y-2)%100, (y-1)%100)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, destPath string) (int64, error) {
			// All calls should use the first pair.
			assert.Contains(t, url, pair1)
			var content string
			if strings.Contains(destPath, "inflow") {
				content = inflowCSV
			} else {
				content = outflowCSV
			}
			if wErr := os.WriteFile(destPath, []byte(content), 0o644); wErr != nil {
				return 0, wErr
			}
			return int64(len(content)), nil
		}).Times(2) // inflow + outflow

	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 2)
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 1)

	ds := &IRSSOIMigration{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestIRSSOIMigration_ProdURL_CorrectURLFormat(t *testing.T) {
	// Verify the production URL format includes the correct prefixes and pairs.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	y := time.Now().Year()
	var attemptedURLs []string

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, _ string) (int64, error) {
			attemptedURLs = append(attemptedURLs, url)
			return 0, errors.New("not found")
		}).Times(4) // 4 pairs for inflow direction, fails before outflow

	ds := &IRSSOIMigration{}
	_, _ = ds.Sync(context.Background(), pool, f, t.TempDir())

	// Should try 4 year pairs for the inflow direction.
	require.Len(t, attemptedURLs, 4)
	for i, url := range attemptedURLs {
		y2 := y - 1 - i
		y1 := y2 - 1
		pair := fmt.Sprintf("%02d%02d", y1%100, y2%100)
		assert.Contains(t, url, "countyinflow"+pair+".csv", "URL at offset %d", i)
		assert.Contains(t, url, "https://www.irs.gov/pub/irs-soi/")
	}
}

func TestIRSSOIMigration_ParseFileOpenError(t *testing.T) {
	// Tests the error path when the CSV file cannot be opened.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// DownloadToFile "succeeds" but doesn't actually write the file.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), nil).Times(1)

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open csv")
}

func TestIRSSOIMigration_ParseFileHeaderError(t *testing.T) {
	// Tests the error path when the CSV header cannot be read (empty file).
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			// Write an empty file.
			return 0, os.WriteFile(destPath, []byte(""), 0o644)
		}).Times(1)

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read header")
}

func TestIRSSOIMigration_UpsertError(t *testing.T) {
	// Tests that a BulkUpsert failure is propagated.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Only the first direction (inflow) will be attempted before the error.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			content := inflowCSV
			return int64(len(content)), os.WriteFile(destPath, []byte(content), 0o644)
		}).Times(1)

	// Make BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(errors.New("db connection lost"))

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse inflow")
}

func TestIRSSOIMigration_ReadRowError(t *testing.T) {
	// Tests the error path when a data row has a parse error.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	malformedCSV := "y1_statefips,y1_countyfips,y2_statefips,y2_countyfips,y2_state,y2_countyname,Return_Num,Exmpt_Num,Adjusted_Gross_Income\n" +
		"48,453,06,037,CA,\"unterminated\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			return int64(len(malformedCSV)), os.WriteFile(destPath, []byte(malformedCSV), 0o644)
		}).Times(1)

	ds := &IRSSOIMigration{baseURL: "http://test.local/irs_soi.csv"}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read row")
}

func TestIRSSOIMigration_ProdURL_OutflowFails(t *testing.T) {
	// Tests the production URL path where inflow succeeds but outflow fails
	// for all pairs.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, destPath string) (int64, error) {
			callCount++
			// Inflow calls succeed (first call for inflow direction).
			if strings.Contains(url, "countyinflow") && strings.Contains(destPath, "inflow") {
				return int64(len(inflowCSV)), os.WriteFile(destPath, []byte(inflowCSV), 0o644)
			}
			// All outflow calls fail.
			return 0, errors.New("not found")
		}).Times(5) // 1 inflow success + 4 outflow failures

	// Inflow succeeds with 2 rows.
	expectBulkUpsert(pool, "fed_data.irs_soi_migration", irsMigrationCols, 2)

	ds := &IRSSOIMigration{}
	_, syncErr := ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, syncErr)
	assert.Contains(t, syncErr.Error(), "tried pairs")
	assert.Contains(t, syncErr.Error(), "outflow")
}
