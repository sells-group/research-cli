package dataset

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestLEHDLODES_Metadata(t *testing.T) {
	ds := &LEHDLODES{}
	assert.Equal(t, "lehd_lodes", ds.Name())
	assert.Equal(t, "fed_data.lehd_lodes", ds.Table())
	assert.Equal(t, Phase3, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestLEHDLODES_ShouldRun(t *testing.T) {
	ds := &LEHDLODES{}

	// Never synced -> should run
	now := time.Date(2024, time.July, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year -> should run (past June release)
	lastYear := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after release -> should not run
	thisYear := time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date -> should not run
	beforeRelease := time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(beforeRelease, &lastYear))
}

func TestLEHDLODES_Sync(t *testing.T) {
	dir := t.TempDir()

	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Mock DownloadToFile: copy the testdata gzip to the requested dest path.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			copyTestFixture(t, gzPath, destPath)
		}).
		Return(int64(1000), nil).
		Times(1)

	// Expect one BulkUpsert call with 2 aggregated county pairs.
	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll("fed_data.lehd_lodes", ".", "_"))
	pool.ExpectBegin()
	pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{tempTable}, lodesCols).WillReturnResult(2)
	pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	pool.ExpectCommit()

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLEHDLODES_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError).
		Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
}

func TestLEHDLODES_404Skipped(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), fmt.Errorf("HTTP 404 not found")).
		Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestLEHDLODES_EmptyFile(t *testing.T) {
	dir := t.TempDir()

	gzPath := filepath.Join("testdata", "lehd_od_empty.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			copyTestFixture(t, gzPath, destPath)
		}).
		Return(int64(100), nil).
		Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// copyTestGZ is a helper that copies testdata gzip to the requested path.
func copyTestGZ(t *testing.T, gzPath string) func(context.Context, string, string) (int64, error) {
	t.Helper()
	return func(_ context.Context, _ string, destPath string) (int64, error) {
		data := readTestFixture(t, gzPath)
		writeTestFixture(t, destPath, data)
		return int64(len(data)), nil
	}
}

func TestLEHDLODES_ProdURL_ProbeSuccess(t *testing.T) {
	// Tests the production URL probe logic (no baseURL set).
	// Probe fails for first year offset, succeeds on second.
	dir := t.TempDir()
	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	y := time.Now().Year()
	probeState := "tx" // first state in our override list

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, url string, destPath string) (int64, error) {
			callCount++
			// First probe (offset=2) fails.
			if callCount == 1 {
				expectedProbeURL := fmt.Sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/od/%s_od_main_JT00_%d.csv.gz",
					probeState, probeState, y-2)
				assert.Equal(t, expectedProbeURL, url)
				return 0, errors.New("not found")
			}
			// Second probe (offset=3) succeeds.
			if callCount == 2 {
				expectedProbeURL := fmt.Sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/od/%s_od_main_JT00_%d.csv.gz",
					probeState, probeState, y-3)
				assert.Equal(t, expectedProbeURL, url)
				// Write then remove the probe file (source does os.Remove).
				data := readTestFixture(t, gzPath)
				writeTestFixture(t, destPath, data)
				return int64(len(data)), nil
			}
			// Third call: actual state download using the probed year.
			expectedURL := fmt.Sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/od/%s_od_main_JT00_%d.csv.gz",
				probeState, probeState, y-3)
			assert.Equal(t, expectedURL, url)
			data := readTestFixture(t, gzPath)
			writeTestFixture(t, destPath, data)
			return int64(len(data)), nil
		}).Times(3) // 2 probes + 1 state download

	// Expect one BulkUpsert for the state's aggregated data.
	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll("fed_data.lehd_lodes", ".", "_"))
	pool.ExpectBegin()
	pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{tempTable}, lodesCols).WillReturnResult(2)
	pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	pool.ExpectCommit()

	ds := &LEHDLODES{
		states: []string{probeState}, // single state to simplify
	} // no baseURL → production path
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLEHDLODES_ProdURL_ProbeAllFail(t *testing.T) {
	// Tests the production URL probe when all years fail. Should use default
	// year and still proceed (warn but don't error).
	dir := t.TempDir()
	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			callCount++
			// First 4 calls are probes (offsets 2-5), all fail.
			if callCount <= 4 {
				return 0, errors.New("not found")
			}
			// Fifth call: actual state download with default year.
			data, _ := os.ReadFile(gzPath)
			return int64(len(data)), os.WriteFile(destPath, data, 0644)
		}).Times(5) // 4 probes + 1 state download

	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll("fed_data.lehd_lodes", ".", "_"))
	pool.ExpectBegin()
	pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{tempTable}, lodesCols).WillReturnResult(2)
	pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	pool.ExpectCommit()

	ds := &LEHDLODES{
		states: []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.Equal(t, 5, callCount)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLEHDLODES_ProdURL_ProbeFirstSuccess(t *testing.T) {
	// Tests the production URL probe when the first year succeeds immediately.
	dir := t.TempDir()
	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			data, _ := os.ReadFile(gzPath)
			return int64(len(data)), os.WriteFile(destPath, data, 0644)
		}).Times(2) // 1 probe + 1 state download

	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll("fed_data.lehd_lodes", ".", "_"))
	pool.ExpectBegin()
	pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{tempTable}, lodesCols).WillReturnResult(2)
	pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	pool.ExpectCommit()

	ds := &LEHDLODES{
		states: []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLEHDLODES_ProdURL_StateDownloadNon404Error(t *testing.T) {
	// Tests the syncState path when a non-404 download error occurs (not skippable).
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	callCount := 0
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string) (int64, error) {
			callCount++
			if callCount == 1 {
				// Probe succeeds (write nothing needed — probe file gets removed).
				return int64(100), nil
			}
			// State download fails with non-404 error.
			return 0, errors.New("connection reset by peer")
		}).Times(2)

	ds := &LEHDLODES{
		states: []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lehd_lodes: download tx")
}

func TestLEHDLODES_OpenGzipError(t *testing.T) {
	// Tests the error path when the downloaded file is not valid gzip.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Write a non-gzip file.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			content := "this is not gzip data"
			return int64(len(content)), os.WriteFile(destPath, []byte(content), 0644)
		}).Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gzip")
}

func TestLEHDLODES_UpsertError(t *testing.T) {
	// Tests that a BulkUpsert failure is propagated.
	dir := t.TempDir()
	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(copyTestGZ(t, gzPath)).Times(1)

	// Make BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(errors.New("db connection lost"))

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestLEHDLODES_ProdURL_MultipleStates(t *testing.T) {
	// Tests production URL path with multiple states where probe succeeds.
	dir := t.TempDir()
	gzPath := filepath.Join("testdata", "lehd_od.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Probe (1 call) + 2 state downloads.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			data, _ := os.ReadFile(gzPath)
			return int64(len(data)), os.WriteFile(destPath, data, 0644)
		}).Times(3)

	// Two BulkUpsert calls (one per state).
	for range 2 {
		tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll("fed_data.lehd_lodes", ".", "_"))
		pool.ExpectBegin()
		pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
		pool.ExpectCopyFrom(pgx.Identifier{tempTable}, lodesCols).WillReturnResult(2)
		pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
		pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
		pool.ExpectCommit()
	}

	ds := &LEHDLODES{
		states: []string{"tx", "ca"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLEHDLODES_OpenFileError(t *testing.T) {
	// Tests the error path when the gz file cannot be opened after download.
	// DownloadToFile "succeeds" but doesn't write the file.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Return success but don't write the file.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(100), nil).Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open tx")
}

func TestLEHDLODES_ReadHeaderError(t *testing.T) {
	// Tests the error path when the gzip CSV has no header row (empty CSV content).
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Write a valid gzip file containing an empty CSV.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			// Create a gzip file with empty content.
			gzFile, cErr := os.Create(destPath)
			if cErr != nil {
				return 0, cErr
			}
			defer gzFile.Close() //nolint:errcheck
			gzW := gzip.NewWriter(gzFile)
			// Write empty content, close to finalize.
			if cErr = gzW.Close(); cErr != nil {
				return 0, cErr
			}
			return 0, nil
		}).Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header")
}

func TestLEHDLODES_ReadRowError(t *testing.T) {
	// Tests the error path when a data row has a parse error.
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			gzFile, cErr := os.Create(destPath)
			if cErr != nil {
				return 0, cErr
			}
			defer gzFile.Close() //nolint:errcheck
			gzW := gzip.NewWriter(gzFile)
			// Valid header + malformed data row.
			csvContent := "w_geocode,h_geocode,s000,sa01,sa02,sa03,se01,se02,se03\n\"unterminated\n"
			if _, wErr := gzW.Write([]byte(csvContent)); wErr != nil {
				return 0, wErr
			}
			if cErr = gzW.Close(); cErr != nil {
				return 0, cErr
			}
			return 0, nil
		}).Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read")
}

func TestLEHDLODES_ShortGeocodes(t *testing.T) {
	// Tests that rows with geocodes shorter than 5 chars are skipped.
	// This is handled inside syncState when len(wGeo) < 5 || len(hGeo) < 5.
	dir := t.TempDir()

	// Create a gzip with short geocodes.
	gzPath := filepath.Join("testdata", "lehd_od_empty.csv.gz")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, destPath string) (int64, error) {
			data, _ := os.ReadFile(gzPath)
			return int64(len(data)), os.WriteFile(destPath, data, 0644)
		}).Times(1)

	ds := &LEHDLODES{
		baseURL: "https://example.com/lodes_tx.csv.gz",
		states:  []string{"tx"},
	}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}
