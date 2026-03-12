package scraper

import (
	"archive/zip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestFCCBroadband_Metadata(t *testing.T) {
	s := &FCCBroadband{}
	assert.Equal(t, "fcc_broadband", s.Name())
	assert.Equal(t, "geo.broadband_coverage", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFCCBroadband_ShouldRun(t *testing.T) {
	s := &FCCBroadband{}
	now := fixedNow() // 2026-03-01 12:00 UTC

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// fixedNow is March, before July release → any synced value means no sync needed.
	recent := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// After July release: now is August 2026, synced before July → should run.
	afterRelease := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(afterRelease, &stale))

	// After July release: synced after July → should not run.
	recentPost := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(afterRelease, &recentPost))
}

func TestFCCBroadband_MissingAPIKey(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: "http://example.com/bdc.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FCC BDC API key required")
}

func TestFCCBroadband_Sync(t *testing.T) {
	csvContent := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n" +
		"481234567001000,50,1000,500,3,30.27,-97.74\n" +
		"481234567002000,40,200,20,2,30.28,-97.75\n"

	zipPath := createTestCSVZip(t, "broadband.csv", csvContent)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBroadbandUpsert(mock, 2)

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFCCBroadband_ParseRow(t *testing.T) {
	colIdx := map[string]int{
		"block_geoid":        0,
		"technology":         1,
		"max_download_speed": 2,
		"max_upload_speed":   3,
		"provider_count":     4,
		"latitude":           5,
		"longitude":          6,
	}
	record := []string{"481234567001000", "50", "1000", "500", "3", "30.27", "-97.74"}

	row, ok := parseBroadbandRow(record, colIdx)
	require.True(t, ok)
	require.Len(t, row, 10)

	assert.Equal(t, "481234567001000", row[0])                        // block_geoid
	assert.Equal(t, "fiber", row[1])                                  // technology
	assert.InDelta(t, 1000.0, row[2].(float64), 0.001)                // max_download
	assert.InDelta(t, 500.0, row[3].(float64), 0.001)                 // max_upload
	assert.Equal(t, 3, row[4])                                        // provider_count
	assert.InDelta(t, 30.27, row[5].(float64), 0.001)                 // latitude
	assert.InDelta(t, -97.74, row[6].(float64), 0.01)                 // longitude
	assert.Equal(t, fccSource, row[7])                                // source
	assert.Equal(t, "fcc_bdc/481234567001000/fiber", row[8].(string)) // source_id
}

func TestFCCBroadband_ParseRow_MissingGEOID(t *testing.T) {
	colIdx := map[string]int{
		"block_geoid": 0,
		"technology":  1,
	}
	record := []string{"", "50"}
	_, ok := parseBroadbandRow(record, colIdx)
	assert.False(t, ok)
}

func TestFCCBroadband_TechMapping(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"10", "dsl"},
		{"40", "cable"},
		{"50", "fiber"},
		{"60", "satellite"},
		{"70", "fixed_wireless"},
		{"71", "fixed_wireless"},
		{"72", "fixed_wireless"},
		{"99", "other"},
		{"", "other"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, fccTechName(tt.code), "code=%q", tt.code)
	}
}

func TestFCCBroadband_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: "http://127.0.0.1:1/bad", apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fcc_broadband: download")
}

func TestFCCBroadband_EmptyCSV(t *testing.T) {
	csvContent := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n"

	zipPath := createTestCSVZip(t, "broadband.csv", csvContent)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFCCBroadband_ContextCancelled(t *testing.T) {
	csvContent := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n" +
		"481234567001000,50,1000,500,3,30.27,-97.74\n"

	zipPath := createTestCSVZip(t, "broadband.csv", csvContent)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFCCBroadband_NoCSVInZip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "test.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw, err := zw.Create("readme.txt")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("no csv"))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV files")
}

func TestFCCBroadband_UpsertError(t *testing.T) {
	csvContent := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n" +
		"481234567001000,50,1000,500,3,30.27,-97.74\n"

	zipPath := createTestCSVZip(t, "broadband.csv", csvContent)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFCCBroadband_EmptyFileCSV(t *testing.T) {
	// Serve a ZIP containing an empty CSV file → header read fails.
	zipPath := createTestCSVZip(t, "empty.csv", "")
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header")
}

func TestFCCBroadband_NoURLSet(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{apiKey: "test-key"} // no downloadURL
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download URL is required")
}

func TestCSVField_OutOfRange(t *testing.T) {
	record := []string{"value0", "value1"}
	colIdx := map[string]int{"missing": 5, "valid": 0}
	assert.Equal(t, "", csvField(record, colIdx, "missing"))
	assert.Equal(t, "", csvField(record, colIdx, "nonexistent"))
	assert.Equal(t, "value0", csvField(record, colIdx, "valid"))
}

func TestCSVFloat_EmptyReturnsZero(t *testing.T) {
	record := []string{""}
	colIdx := map[string]int{"val": 0}
	assert.Equal(t, 0.0, csvFloat(record, colIdx, "val"))
	assert.Equal(t, 0.0, csvFloat(record, colIdx, "missing"))
}

func TestFCCBroadband_ExtractError(t *testing.T) {
	// Serve a corrupt ZIP that cannot be extracted.
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "corrupt.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("not a zip"), 0o644))
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestFCCBroadband_MultipleCSVFiles(t *testing.T) {
	csv1 := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n" +
		"481234567001000,50,1000,500,3,30.27,-97.74\n"
	csv2 := "block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n" +
		"481234567002000,40,200,20,2,30.28,-97.75\n"

	dir := t.TempDir()
	zipPath := filepath.Join(dir, "test.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw1, _ := zw.Create("state1.csv")
	_, _ = fw1.Write([]byte(csv1))
	fw2, _ := zw.Create("state2.csv")
	_, _ = fw2.Write([]byte(csv2))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBroadbandUpsert(mock, 2)

	s := &FCCBroadband{downloadURL: srv.URL, apiKey: "test-key"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
}

func TestFCCBroadband_ProcessCSV_EmptyCSV(t *testing.T) {
	// Write a CSV with only a header line to a temp file.
	dir := t.TempDir()
	csvPath := dir + "/empty.csv"
	require.NoError(t, os.WriteFile(csvPath, []byte(
		"block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n",
	), 0o644))

	var batch [][]any
	flushCalled := false
	flush := func() error {
		flushCalled = true
		return nil
	}

	s := &FCCBroadband{}
	err := s.processCSV(csvPath, &batch, flush)
	require.NoError(t, err)
	assert.Empty(t, batch)
	assert.False(t, flushCalled, "flush should not be called for an empty CSV")
}

func TestFCCBroadband_ProcessCSV_FlushError(t *testing.T) {
	// Create a CSV with enough rows to trigger a mid-batch flush, then have flush fail.
	dir := t.TempDir()
	csvPath := dir + "/big.csv"
	var sb strings.Builder
	sb.WriteString("block_geoid,technology,max_download_speed,max_upload_speed,provider_count,latitude,longitude\n")
	for i := 0; i < fccBatchSize+1; i++ {
		fmt.Fprintf(&sb, "48%012d,50,1000,500,3,30.27,-97.74\n", i)
	}
	require.NoError(t, os.WriteFile(csvPath, []byte(sb.String()), 0o644))

	var batch [][]any
	flush := func() error {
		return eris.New("flush failed")
	}

	s := &FCCBroadband{}
	err := s.processCSV(csvPath, &batch, flush)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush failed")
}

func TestFCCBroadband_ProcessCSV_OpenError(t *testing.T) {
	var batch [][]any
	flush := func() error { return nil }

	s := &FCCBroadband{}
	err := s.processCSV("/nonexistent/path.csv", &batch, flush)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open CSV")
}

func TestCSVInt_EmptyReturnsZero(t *testing.T) {
	record := []string{""}
	colIdx := map[string]int{"val": 0}
	assert.Equal(t, 0, csvInt(record, colIdx, "val"))
	assert.Equal(t, 0, csvInt(record, colIdx, "missing"))
}

// ---------- Helpers ----------

// expectBroadbandUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.broadband_coverage table.
func expectBroadbandUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_broadband_coverage"}, broadbandCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

// createTestCSVZip creates a ZIP file containing a single CSV with the given content.
func createTestCSVZip(t *testing.T, csvName, csvContent string) string {
	t.Helper()
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "test.zip")

	zf, err := os.Create(zipPath)
	require.NoError(t, err)

	zw := zip.NewWriter(zf)
	fw, err := zw.Create(csvName)
	require.NoError(t, err)
	_, err = fw.Write([]byte(csvContent))
	require.NoError(t, err)

	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	return zipPath
}
