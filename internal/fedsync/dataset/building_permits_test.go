package dataset

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func init() {
	// Ensure zap global logger is initialized for tests.
	if zap.L() == zap.NewNop() {
		l, _ := zap.NewDevelopment()
		zap.ReplaceGlobals(l)
	}
}

func TestBuildingPermits_Metadata(t *testing.T) {
	ds := &BuildingPermits{}

	assert.Equal(t, "building_permits", ds.Name())
	assert.Equal(t, "fed_data.building_permits", ds.Table())
	assert.Equal(t, Phase2, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestBuildingPermits_ShouldRun(t *testing.T) {
	ds := &BuildingPermits{}

	t.Run("nil lastSync returns true", func(t *testing.T) {
		assert.True(t, ds.ShouldRun(time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC), nil))
	})

	t.Run("recent sync returns false", func(t *testing.T) {
		recent := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, ds.ShouldRun(time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC), &recent))
	})

	t.Run("before release month returns false", func(t *testing.T) {
		old := time.Date(2025, time.April, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, ds.ShouldRun(time.Date(2026, time.February, 1, 0, 0, 0, 0, time.UTC), &old))
	})
}

// copyTestCSV returns a RunAndReturn func that copies a testdata CSV to the requested path.
func copyTestCSV(t *testing.T, srcPath string) func(context.Context, string, string) (int64, error) {
	t.Helper()
	return func(_ context.Context, _ string, path string) (int64, error) {
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return 0, err
		}
		return int64(len(data)), os.WriteFile(path, data, 0o644)
	}
}

func TestBuildingPermits_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	testCSV := filepath.Join("testdata", "building_permits.csv")
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(copyTestCSV(t, testCSV)).Once()

	expectBulkUpsert(pool, "fed_data.building_permits", bpsCols, 3)

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestBuildingPermits_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	dlErr := errors.New("network timeout")
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), dlErr).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestBuildingPermits_EmptyCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// Write a CSV with only header rows (two-row header + blank line, no data).
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			content := "Survey,FIPS,FIPS,Region,Division,County,,1-unit,,,2-units,,,3-4 units,,,5+ units,,\nDate,State,County,Code,Code,Name,Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value\n\n"
			return int64(len(content)), os.WriteFile(path, []byte(content), 0o644)
		}).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestBuildingPermits_PadFIPS(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// Use the testdata CSV which has state "6" and county "37" that need padding.
	testCSV := filepath.Join("testdata", "building_permits.csv")
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(copyTestCSV(t, testCSV)).Once()

	// Capture the batch passed to BulkUpsert by intercepting the CopyFrom call.
	// The third row has state "6" → "06" and county "37" → "037".
	expectBulkUpsert(pool, "fed_data.building_permits", bpsCols, 3)

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	require.NoError(t, pool.ExpectationsWereMet())

	// Verify padding logic directly.
	assert.Equal(t, "06", padLeft("6", 2))
	assert.Equal(t, "037", padLeft("37", 3))
	assert.Equal(t, "48", padLeft("48", 2))
	assert.Equal(t, "453", padLeft("453", 3))
}

func TestBuildingPermits_ProdURL_FallbackSuccess(t *testing.T) {
	// Tests the production URL fallback loop (no baseURL set).
	// First 2 year attempts fail, third succeeds.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()
	testCSV := filepath.Join("testdata", "building_permits.csv")

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, path string) (int64, error) {
			callCount++
			if callCount <= 2 {
				return 0, fmt.Errorf("HTTP 404 not found for %s", url)
			}
			// Third attempt succeeds — copy the test CSV.
			data, rErr := os.ReadFile(testCSV)
			if rErr != nil {
				return 0, rErr
			}
			return int64(len(data)), os.WriteFile(path, data, 0o644)
		}).Times(3)

	expectBulkUpsert(pool, "fed_data.building_permits", bpsCols, 3)

	ds := &BuildingPermits{} // no baseURL → production path
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.Equal(t, 3, callCount)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestBuildingPermits_ProdURL_AllYearsFail(t *testing.T) {
	// Tests the production URL fallback loop when all 4 year attempts fail.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("not found")).Times(4)

	ds := &BuildingPermits{} // no baseURL → production path
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tried 4 years")
}

func TestBuildingPermits_ProdURL_FirstYearSuccess(t *testing.T) {
	// Tests that the production URL loop succeeds on the first attempt.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()
	testCSV := filepath.Join("testdata", "building_permits.csv")

	y := time.Now().Year()
	expectedURL := fmt.Sprintf("https://www2.census.gov/econ/bps/County/co%da.txt", y-1)

	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return url == expectedURL
	}), mock.Anything).
		RunAndReturn(copyTestCSV(t, testCSV)).Once()

	expectBulkUpsert(pool, "fed_data.building_permits", bpsCols, 3)

	ds := &BuildingPermits{}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestBuildingPermits_OpenCSVError(t *testing.T) {
	// Tests the error path when the CSV file cannot be opened (e.g., download
	// wrote nothing but returned no error, then file doesn't exist).
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// DownloadToFile "succeeds" but doesn't write the file.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), nil).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open csv")
}

func TestBuildingPermits_HeaderOnlyEOF(t *testing.T) {
	// Tests the early EOF path — file has only one header row (EOF on second read).
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			content := "Survey,FIPS,FIPS,Region,Division,County\n"
			return int64(len(content)), os.WriteFile(path, []byte(content), 0o644)
		}).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestBuildingPermits_SkipEmptyFIPS(t *testing.T) {
	// Tests that rows with empty state or county FIPS are skipped.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// 3 header rows + 2 data rows: one with empty state FIPS, one with empty county FIPS.
	csv := "H1,,,,,,,,,,,,,,,,,,\nH2,,,,,,,,,,,,,,,,,,\n\n" +
		",48,,3,7,Travis County,1200,1200,300000,25,50,20000,10,30,10000,5,220,20000\n" +
		"2024,,453,3,7,Dallas County,1500,1500,400000,50,100,40000,15,50,20000,10,350,40000\n"
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(len(csv)), os.WriteFile(path, []byte(csv), 0o644)
		}).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestBuildingPermits_UpsertError(t *testing.T) {
	// Tests that a BulkUpsert failure is propagated.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()
	testCSV := filepath.Join("testdata", "building_permits.csv")

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(copyTestCSV(t, testCSV)).Once()

	// Make BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(errors.New("db connection lost"))

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestBuildingPermits_CleanNumeric(t *testing.T) {
	assert.Equal(t, "300000", cleanNumeric("300,000"))
	assert.Equal(t, "1000", cleanNumeric(" 1,000 "))
	assert.Equal(t, "", cleanNumeric(""))
}

func TestBuildingPermits_ProdURL_CorrectURLFormat(t *testing.T) {
	// Verify the production URL format includes the correct year offsets.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	y := time.Now().Year()
	var attemptedURLs []string

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, _ string) (int64, error) {
			attemptedURLs = append(attemptedURLs, url)
			return 0, errors.New("not found")
		}).Times(4)

	ds := &BuildingPermits{}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)

	// Verify all 4 URLs were tried with correct years.
	require.Len(t, attemptedURLs, 4)
	for i, url := range attemptedURLs {
		expected := fmt.Sprintf("https://www2.census.gov/econ/bps/County/co%da.txt", y-1-i)
		assert.Equal(t, expected, url, "URL at offset %d", i+1)
	}
}

func TestBuildingPermits_HeaderParseError(t *testing.T) {
	// Tests the header skip error path (malformed CSV that produces a read error).
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// Write a file with invalid CSV that will cause a parse error on the header row.
	// Using a bare quote that can't be parsed.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			// FieldsPerRecord=-1 means field count won't error, but an unterminated
			// quote will cause a parse error.
			content := "\"unterminated\n"
			return int64(len(content)), os.WriteFile(path, []byte(content), 0o644)
		}).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "skip header row")
}

func TestBuildingPermits_DataRowReadError(t *testing.T) {
	// Tests the error path when a data row has a read error (unterminated quote).
	// The 3 header rows must be clean so they're skipped successfully;
	// the unterminated quote is placed in the 4th (data) row.
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	// 2 header rows + 1 blank line (skipped by the header loop) + unterminated quote
	// in a data row. The blank line counts as one read, so the 3 reads skip
	// lines 1, 2, and 3 (blank). Line 4 is the malformed data row.
	csvContent := "H1,H2,H3\nS1,S2,S3\nblank\n\"unterminated\n"
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(len(csvContent)), os.WriteFile(path, []byte(csvContent), 0o644)
		}).Once()

	ds := &BuildingPermits{baseURL: "https://example.com/bps.txt"}
	_, err = ds.Sync(context.Background(), pool, f, tempDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read row")
}
