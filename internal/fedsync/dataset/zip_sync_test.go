package dataset

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

// currentDataYear returns time.Now().Year()-1, matching the lag logic used by
// CBP, SUSB, OEWS, and QCEW Sync methods.
func currentDataYear() int {
	return time.Now().Year() - 1
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// createTestZip creates a ZIP file at dir/zipName containing a single file
// csvName with the given csvContent. Returns the full path to the ZIP.
func createTestZip(t *testing.T, dir, zipName, csvName, csvContent string) string {
	t.Helper()
	zipPath := filepath.Join(dir, zipName)
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	f, err := w.Create(csvName)
	require.NoError(t, err)
	_, err = f.Write([]byte(csvContent))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())
	return zipPath
}

// createTestZipMulti creates a ZIP with multiple CSV files.
func createTestZipMulti(t *testing.T, dir, zipName string, files map[string]string) string {
	t.Helper()
	zipPath := filepath.Join(dir, zipName)
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	for name, content := range files {
		f, err := w.Create(name)
		require.NoError(t, err)
		_, err = f.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())
	return zipPath
}

// mockDownloadToFile sets up a DownloadToFile mock that copies a pre-built ZIP
// to whatever destination path the caller requests. Matches any URL.
func mockDownloadToFile(f *fetchermocks.MockFetcher, zipPath string) *fetchermocks.MockFetcher_DownloadToFile_Call {
	return f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			data, err := os.ReadFile(zipPath)
			if err != nil {
				panic(fmt.Sprintf("mockDownloadToFile: ReadFile %s: %v", zipPath, err))
			}
			if err := os.WriteFile(destPath, data, 0644); err != nil {
				panic(fmt.Sprintf("mockDownloadToFile: WriteFile %s: %v", destPath, err))
			}
		}).
		Return(int64(1000), nil)
}

// expectBulkUpsertZip sets up pgxmock expectations for one db.BulkUpsert call.
// BulkUpsert does: Begin -> CREATE TEMP TABLE -> CopyFrom -> INSERT ON CONFLICT -> Commit.
func expectBulkUpsertZip(m pgxmock.PgxPoolIface, table string, cols []string, n int64) {
	tempTable := fmt.Sprintf("_tmp_upsert_%s", replaceDotsUnderscore(table))
	m.ExpectBegin()
	m.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	m.ExpectCopyFrom(pgx.Identifier{tempTable}, cols).WillReturnResult(n)
	m.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", n))
	m.ExpectCommit()
}

func replaceDotsUnderscore(s string) string {
	out := make([]byte, len(s))
	for i := range s {
		if s[i] == '.' {
			out[i] = '_'
		} else {
			out[i] = s[i]
		}
	}
	return string(out)
}

// ===========================================================================
// CBP tests
// ===========================================================================

var cbpCols = []string{"year", "fips_state", "fips_county", "naics", "emp", "emp_nf", "qp1", "qp1_nf", "ap", "ap_nf", "est"}

// cbpCSV builds a minimal valid CBP CSV with a header and one row for a relevant NAICS.
// Column names match what the Census actually ships (fipstate, fipscty, naics, emp, ...).
const cbpCSVHeader = "fipstate,fipscty,naics,emp,emp_nf,qp1,qp1_nf,ap,ap_nf,est\n"

func TestCBP_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	// Build a ZIP with one relevant row (NAICS 523110 -> starts with 52).
	csvContent := cbpCSVHeader +
		"36,001,523110,150,A,5000,B,20000,C,10\n" +
		"36,003,523120,200,,6000,,25000,,15\n"

	// CBP loops from cbpStartYear (2019) to currentYear (time.Now().Year()-1).
	// We need one ZIP per year. To keep the test fast, we just provide the
	// same ZIP content for every DownloadToFile call.
	zipPath := createTestZip(t, dir, "cbp.zip", "cbp19co.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// The CBP Sync loop calls DownloadToFile once per year.
	// Each year produces 2 rows -> one BulkUpsert per year.
	numYears := currentDataYear() - cbpStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.cbp_data", cbpCols, 2)
	}

	ds := &CBP{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCBP_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("network timeout"))

	ds := &CBP{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestCBP_Sync_FiltersIrrelevantNAICS(t *testing.T) {
	dir := t.TempDir()

	// NAICS 311 (food mfg) is NOT in the relevant list -> should be filtered out.
	// NAICS 523110 (securities) IS relevant.
	csvContent := cbpCSVHeader +
		"36,001,311110,500,,10000,,50000,,20\n" +
		"36,001,523110,150,,5000,,20000,,10\n"

	zipPath := createTestZip(t, dir, "cbp.zip", "cbp19co.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - cbpStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	// Only 1 row passes the NAICS filter per year.
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.cbp_data", cbpCols, 1)
	}

	ds := &CBP{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCBP_ProcessZip_NoCSV(t *testing.T) {
	dir := t.TempDir()

	// Create a ZIP with no CSV file inside.
	zipPath := filepath.Join(dir, "empty.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	f, err := w.Create("readme.txt.bak") // not .csv or .txt
	require.NoError(t, err)
	_, _ = f.Write([]byte("not a csv"))
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &CBP{}
	_, err = ds.processZip(context.Background(), pool, zipPath, 2023)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV found")
}

// ===========================================================================
// SUSB tests
// ===========================================================================

var susbCols = []string{"year", "fips_state", "naics", "entrsizedscr", "firm", "estb", "empl", "payr"}

const susbCSVHeader = "statefips,naics,entrsizedscr,firm,estb,empl,payr\n"

func TestSUSB_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	csvContent := susbCSVHeader +
		"06,523110,1: Total,100,120,5000,250000\n" +
		"36,541110,2: <5,50,50,200,100000\n"

	zipPath := createTestZip(t, dir, "susb.zip", "us_state_totals.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - susbStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.susb_data", susbCols, 2)
	}

	ds := &SUSB{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSUSB_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("connection refused"))

	ds := &SUSB{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestSUSB_Sync_FiltersIrrelevantNAICS(t *testing.T) {
	dir := t.TempDir()

	// NAICS 311 = food mfg -> irrelevant. NAICS 523 = securities -> relevant.
	csvContent := susbCSVHeader +
		"06,311110,1: Total,100,120,5000,250000\n" +
		"06,523110,1: Total,50,60,2000,100000\n"

	zipPath := createTestZip(t, dir, "susb.zip", "us_state_totals.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - susbStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.susb_data", susbCols, 1)
	}

	ds := &SUSB{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSUSB_ProcessZip_NoCSV(t *testing.T) {
	dir := t.TempDir()

	zipPath := filepath.Join(dir, "empty.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	f, err := w.Create("metadata.json")
	require.NoError(t, err)
	_, _ = f.Write([]byte("{}"))
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &SUSB{}
	_, err = ds.processZip(context.Background(), pool, zipPath, 2023)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV found")
}

// ===========================================================================
// OEWS tests
// ===========================================================================

var oewsCols = []string{"area_code", "area_type", "naics", "occ_code", "year", "tot_emp", "h_mean", "a_mean", "h_median", "a_median"}

// OEWS looks for a file containing "nat" in the name.
const oewsCSVHeader = "area,area_type,naics,occ_code,tot_emp,h_mean,a_mean,h_median,a_median\n"

func TestOEWS_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	csvContent := oewsCSVHeader +
		"99000,1,523110,13-2051,1200,45.50,94640,42.00,87360\n" +
		"99000,1,541110,15-1252,800,55.25,114920,50.10,104210\n"

	// OEWS looks for filename containing "nat" in the ZIP.
	zipPath := createTestZip(t, dir, "oews.zip", "nat40_dl.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - oewsStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.oews_data", oewsCols, 2)
	}

	ds := &OEWS{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestOEWS_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("server error"))

	ds := &OEWS{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestOEWS_Sync_FiltersIrrelevantNAICS(t *testing.T) {
	dir := t.TempDir()

	// NAICS 311 -> irrelevant, 523110 -> relevant
	csvContent := oewsCSVHeader +
		"99000,1,311110,13-2051,1200,45.50,94640,42.00,87360\n" +
		"99000,1,523110,13-2051,800,55.25,114920,50.10,104210\n"

	zipPath := createTestZip(t, dir, "oews.zip", "nat40_dl.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - oewsStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.oews_data", oewsCols, 1)
	}

	ds := &OEWS{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestOEWS_Sync_FallbackCSV(t *testing.T) {
	dir := t.TempDir()

	// OEWS first looks for a file with "nat" in the name; if not found,
	// falls back to the first .csv. Put the data in a non-"nat" named file.
	csvContent := oewsCSVHeader +
		"99000,1,523110,13-2051,1200,45.50,94640,42.00,87360\n"

	zipPath := createTestZip(t, dir, "oews.zip", "alldata_dl.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - oewsStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.oews_data", oewsCols, 1)
	}

	ds := &OEWS{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestOEWS_ProcessZip_NoCSV(t *testing.T) {
	dir := t.TempDir()

	zipPath := filepath.Join(dir, "empty.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	fe, err := w.Create("readme.pdf")
	require.NoError(t, err)
	_, _ = fe.Write([]byte("pdf"))
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &OEWS{}
	_, err = ds.processZip(context.Background(), pool, zipPath, 2023)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV found")
}

// ===========================================================================
// QCEW tests
// ===========================================================================

var qcewCols = []string{"area_fips", "own_code", "industry_code", "year", "qtr", "month1_emplvl", "month2_emplvl", "month3_emplvl", "total_qtrly_wages", "avg_wkly_wage", "qtrly_estabs"}

const qcewCSVHeader = "area_fips,own_code,industry_code,agglvl_code,size_code,year,qtr,month1_emplvl,month2_emplvl,month3_emplvl,total_qtrly_wages,avg_wkly_wage,qtrly_estabs\n"

func TestQCEW_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	// QCEW isRelevantFile checks for NAICS prefix numbers in the filename.
	// "52 NAICS 52.csv" matches prefix "52".
	csvContent := qcewCSVHeader +
		"36000,5,523110,70,0,2023,1,1500,1550,1600,75000000,3800,120\n" +
		"36000,5,523110,70,0,2023,2,1600,1650,1700,80000000,3900,125\n"

	files := map[string]string{
		"2023.q1-q4 52 NAICS 52.csv": csvContent,
	}

	zipPath := createTestZipMulti(t, dir, "qcew.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - qcewStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	// Each year processes one relevant CSV with 2 rows -> one BulkUpsert.
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 2)
	}

	ds := &QCEW{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestQCEW_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("timeout"))

	ds := &QCEW{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestQCEW_Sync_SkipsAnnualAggregate(t *testing.T) {
	dir := t.TempDir()

	// qtr=0 is annual aggregate and should be skipped.
	csvContent := qcewCSVHeader +
		"36000,5,523110,70,0,2023,0,6000,6200,6500,300000000,3850,120\n" +
		"36000,5,523110,70,0,2023,1,1500,1550,1600,75000000,3800,120\n"

	files := map[string]string{
		"2023.q1-q4 52 NAICS 52.csv": csvContent,
	}

	zipPath := createTestZipMulti(t, dir, "qcew.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - qcewStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	// Only 1 row passes per year (qtr=0 is skipped).
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 1)
	}

	ds := &QCEW{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestQCEW_Sync_SkipsIrrelevantFiles(t *testing.T) {
	dir := t.TempDir()

	// The "31 NAICS 31.csv" file is NOT relevant (food mfg).
	// The "52 NAICS 52.csv" IS relevant (finance).
	relevantCSV := qcewCSVHeader +
		"36000,5,523110,70,0,2023,1,1500,1550,1600,75000000,3800,120\n"

	irrelevantCSV := qcewCSVHeader +
		"36000,5,311110,70,0,2023,1,3000,3100,3200,100000000,2000,500\n"

	files := map[string]string{
		"2023.q1-q4 31 NAICS 31.csv": irrelevantCSV,
		"2023.q1-q4 52 NAICS 52.csv": relevantCSV,
	}

	zipPath := createTestZipMulti(t, dir, "qcew.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - qcewStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	// Only the relevant file's rows get upserted. The irrelevant file is
	// skipped by isRelevantFile, so its NAICS-relevant rows never get parsed.
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 1)
	}

	ds := &QCEW{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestQCEW_Sync_MultipleRelevantFiles(t *testing.T) {
	dir := t.TempDir()

	// QCEW processes ALL relevant CSVs in one ZIP (unlike CBP/SUSB/OEWS which stop at first).
	csv52 := qcewCSVHeader +
		"36000,5,523110,70,0,2023,1,1500,1550,1600,75000000,3800,120\n"
	csv54 := qcewCSVHeader +
		"36000,5,541110,70,0,2023,1,2000,2100,2200,90000000,4200,200\n"

	files := map[string]string{
		"2023.q1-q4 52 NAICS 52.csv": csv52,
		"2023.q1-q4 54 NAICS 54.csv": csv54,
	}

	zipPath := createTestZipMulti(t, dir, "qcew.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - qcewStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	// 2 CSV files per year, 1 row each -> 2 BulkUpsert calls per year.
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 1)
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 1)
	}

	ds := &QCEW{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestQCEW_Sync_FiltersIrrelevantNAICS(t *testing.T) {
	dir := t.TempDir()

	// The file is relevant (52 NAICS 52.csv), but one row has NAICS 311 which
	// fails IsRelevantNAICS. Only the 523 row should pass.
	csvContent := qcewCSVHeader +
		"36000,5,311110,70,0,2023,1,3000,3100,3200,100000000,2000,500\n" +
		"36000,5,523110,70,0,2023,1,1500,1550,1600,75000000,3800,120\n"

	files := map[string]string{
		"2023.q1-q4 52 NAICS 52.csv": csvContent,
	}

	zipPath := createTestZipMulti(t, dir, "qcew.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - qcewStartYear + 1

	mockDownloadToFile(f, zipPath).Times(numYears)

	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.qcew_data", qcewCols, 1)
	}

	ds := &QCEW{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}
