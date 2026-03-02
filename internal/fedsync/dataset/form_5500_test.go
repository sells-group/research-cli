package dataset

import (
	"archive/zip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestForm5500_Metadata(t *testing.T) {
	ds := &Form5500{}
	assert.Equal(t, "form_5500", ds.Name())
	assert.Equal(t, "fed_data.form_5500", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestForm5500_ShouldRun(t *testing.T) {
	ds := &Form5500{}

	// Never synced -> should run
	now := time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year, now past July -> should run
	lastYear := time.Date(2023, time.September, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after July release -> should not run
	thisYear := time.Date(2024, time.July, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date (June) -> should not run
	beforeRelease := time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(beforeRelease, &lastYear))
}

// ---------------------------------------------------------------------------
// Minimal test CSV headers (subset of real DOL columns, all are valid)
// ---------------------------------------------------------------------------

const testMainCSVHeader = "ACK_ID,SPONS_DFE_EIN,SPONSOR_DFE_NAME,PLAN_NAME,TOT_PARTCP_BOY_CNT,DATE_RECEIVED\n"
const testSFCSVHeader = "ACK_ID,SF_SPONS_EIN,SF_SPONSOR_NAME,SF_TOT_ASSETS_EOY_AMT,DATE_RECEIVED\n"
const testSchHCSVHeader = "ACK_ID,SCH_H_EIN,TOT_ASSETS_BOY_AMT,TOT_ASSETS_EOY_AMT,NET_ASSETS_EOY_AMT\n"
const testSchCCSVHeader = "ACK_ID,ROW_ORDER,PROVIDER_ELIGIBLE_NAME,PROVIDER_ELIGIBLE_EIN\n"

// Column slices matching the lowercased test headers (in header order).
var testMainCols = []string{"ack_id", "spons_dfe_ein", "sponsor_dfe_name", "plan_name", "tot_partcp_boy_cnt", "date_received"}
var testSFCols = []string{"ack_id", "sf_spons_ein", "sf_sponsor_name", "sf_tot_assets_eoy_amt", "date_received"}
var testSchHCols = []string{"ack_id", "sch_h_ein", "tot_assets_boy_amt", "tot_assets_eoy_amt", "net_assets_eoy_amt"}
var testSchCCols = []string{"ack_id", "row_order", "provider_eligible_name", "provider_eligible_ein"}

func TestForm5500_ParseCSVDynamic_MainForm(t *testing.T) {
	csvContent := testMainCSVHeader +
		"20240101000001,123456789,ACME CORP,ACME 401K,100,2024-01-15\n" +
		"20240101000002,987654321,TEST LLC,TEST PLAN,50,2024-02-20\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500", testMainCols, 2)

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_SkipsEmptyEIN(t *testing.T) {
	csvContent := testMainCSVHeader +
		"20240101000001,,NO EIN CORP,PLAN A,100,2024-01-15\n" +
		"20240101000002,987654321,HAS EIN LLC,PLAN B,50,2024-02-20\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500", testMainCols, 1)

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_EmptyCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(testMainCSVHeader), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestForm5500_ParseCSVDynamic_ShortForm(t *testing.T) {
	csvContent := testSFCSVHeader +
		"20240101000099,111222333,SMALL BIZ,110000.50,2024-03-01\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500_sf", testSFCols, 1)

	ds := &Form5500{}
	spec := form5500Specs[zipShortForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_ScheduleH(t *testing.T) {
	csvContent := testSchHCSVHeader +
		"20240101000001,123456789,5000000,5500000,5400000\n" +
		"20240101000002,987654321,1000000,1100000,1050000\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500_schedule_h", testSchHCols, 2)

	ds := &Form5500{}
	spec := form5500Specs[zipScheduleH]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_ScheduleC(t *testing.T) {
	csvContent := testSchCCSVHeader +
		"20240101000001,1,FIDELITY INVESTMENTS,043523567\n" +
		"20240101000001,2,VANGUARD GROUP,231956272\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500_providers", testSchCCols, 2)

	ds := &Form5500{}
	spec := form5500Specs[zipScheduleC]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_IgnoresUnknownColumns(t *testing.T) {
	// CSV has an extra column "FUTURE_COLUMN" that isn't in validCols — should be ignored.
	csvContent := "ACK_ID,SPONS_DFE_EIN,FUTURE_COLUMN,SPONSOR_DFE_NAME\n" +
		"20240101000001,123456789,some_value,ACME CORP\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Only the 3 valid columns (ack_id, spons_dfe_ein, sponsor_dfe_name) should be passed.
	expectedCols := []string{"ack_id", "spons_dfe_ein", "sponsor_dfe_name"}
	expectBulkUpsertZip(pool, "fed_data.form_5500", expectedCols, 1)

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ParseCSVDynamic_NumericParsing(t *testing.T) {
	// Verify that _amt columns return nil for empty values and float64 for valid numbers.
	csvContent := testSchHCSVHeader +
		"20240101000001,123456789,,5500000.99,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500_schedule_h", testSchHCols, 1)

	ds := &Form5500{}
	spec := form5500Specs[zipScheduleH]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	mainCSVContent := testMainCSVHeader +
		"20240101000001,123456789,ACME CORP,ACME 401K,100,2024-01-15\n"
	sfCSVContent := testSFCSVHeader +
		"20240101000099,111222333,SMALL BIZ,110000,2024-03-01\n"
	schHContent := testSchHCSVHeader +
		"20240101000001,123456789,5000000,5500000,5400000\n"
	schCContent := testSchCCSVHeader +
		"20240101000001,1,FIDELITY,043523567\n"

	mainZip := createTestZip(t, dir, "main.zip", "f_5500_2024_latest.csv", mainCSVContent)
	sfZip := createTestZip(t, dir, "sf.zip", "f_5500_sf_2024_latest.csv", sfCSVContent)
	schHZip := createTestZip(t, dir, "sch_h.zip", "f_sch_h_2024_latest.csv", schHContent)
	schCZip := createTestZip(t, dir, "sch_c.zip", "f_sch_c_part1_item1_2024_latest.csv", schCContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - form5500StartYear + 1

	// Mock 4 downloads per year.
	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "F_5500_SF_")
	}), mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			data, _ := os.ReadFile(sfZip)
			_ = os.WriteFile(destPath, data, 0644)
		}).
		Return(int64(1000), nil).Times(numYears)

	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "F_SCH_H_")
	}), mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			data, _ := os.ReadFile(schHZip)
			_ = os.WriteFile(destPath, data, 0644)
		}).
		Return(int64(1000), nil).Times(numYears)

	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "F_SCH_C_")
	}), mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			data, _ := os.ReadFile(schCZip)
			_ = os.WriteFile(destPath, data, 0644)
		}).
		Return(int64(1000), nil).Times(numYears)

	// Main form URL: contains F_5500_ but NOT F_5500_SF_ or F_SCH_
	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "/F_5500_") && !strings.Contains(url, "F_5500_SF_")
	}), mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			data, _ := os.ReadFile(mainZip)
			_ = os.WriteFile(destPath, data, 0644)
		}).
		Return(int64(1000), nil).Times(numYears)

	// Each year: 4 BulkUpsert calls, each to its own table.
	for i := 0; i < numYears; i++ {
		expectBulkUpsertZip(pool, "fed_data.form_5500", testMainCols, 1)
		expectBulkUpsertZip(pool, "fed_data.form_5500_sf", testSFCols, 1)
		expectBulkUpsertZip(pool, "fed_data.form_5500_schedule_h", testSchHCols, 1)
		expectBulkUpsertZip(pool, "fed_data.form_5500_providers", testSchCCols, 1)
	}

	ds := &Form5500{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(4)*int64(numYears), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("network timeout"))

	ds := &Form5500{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestForm5500_ProcessZip_NoCSV(t *testing.T) {
	dir := t.TempDir()

	// Create a ZIP with no CSV file.
	zipPath := filepath.Join(dir, "empty.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	fe, err := w.Create("readme.txt")
	require.NoError(t, err)
	_, _ = fe.Write([]byte("not a csv"))
	require.NoError(t, w.Close())
	require.NoError(t, zf.Close())

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &Form5500{}
	_, err = ds.processZip(context.Background(), pool, zipPath, zipMainForm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV found")
}

func TestColTypeFor(t *testing.T) {
	tests := []struct {
		name     string
		expected colType
	}{
		{"tot_assets_eoy_amt", colNumeric},
		{"sf_net_assets_boy_amt", colNumeric},
		{"tot_partcp_boy_cnt", colInt},
		{"num_sch_a_attached_cnt", colInt},
		{"partcp_account_bal_cnt_boy", colInt},
		{"row_order", colInt},
		{"ack_id", colText},
		{"sponsor_dfe_name", colText},
		{"filing_status", colText},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, colTypeFor(tt.name), "colTypeFor(%q)", tt.name)
	}
}

func TestParseValue(t *testing.T) {
	// Text
	assert.Equal(t, "hello", parseValue("hello", colText))
	assert.Equal(t, "", parseValue("", colText))

	// Numeric
	assert.Equal(t, 5500000.99, parseValue("5500000.99", colNumeric))
	assert.Nil(t, parseValue("", colNumeric))
	assert.Nil(t, parseValue("  ", colNumeric))
	assert.Nil(t, parseValue("not_a_number", colNumeric))

	// Integer
	assert.Equal(t, int64(100), parseValue("100", colInt))
	assert.Nil(t, parseValue("", colInt))
	assert.Nil(t, parseValue("abc", colInt))
}

func TestForm5500_TableSpecs_Complete(t *testing.T) {
	// Verify all 4 zip types have specs.
	for _, zt := range []form5500ZipType{zipMainForm, zipShortForm, zipScheduleH, zipScheduleC} {
		spec, ok := form5500Specs[zt]
		require.True(t, ok, "missing spec for zip type %d", zt)
		assert.NotEmpty(t, spec.table)
		assert.NotEmpty(t, spec.conflictKeys)
		assert.NotEmpty(t, spec.validCols)
		assert.NotEmpty(t, spec.requireACKID)
		// ack_id must be in valid cols
		assert.True(t, spec.validCols["ack_id"], "ack_id missing from %s valid cols", spec.table)
	}
}

func TestForm5500_SyncYear_404Skip(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	// All downloads return 404 → should skip gracefully.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("status 404: not found")).
		Times(4)

	ds := &Form5500{}
	rows, err := ds.syncYear(context.Background(), pool, f, t.TempDir(), 2099, zap.NewNop())
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestForm5500_ParseCSVDynamic_NoValidColumns(t *testing.T) {
	// CSV header with only unknown columns.
	csvContent := "UNKNOWN_COL_A,UNKNOWN_COL_B\nfoo,bar\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	_, err = ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no valid columns")
}

func TestForm5500_ParseCSVDynamic_SkipsEmptyAckID(t *testing.T) {
	csvContent := testMainCSVHeader +
		",123456789,ACME CORP,ACME 401K,100,2024-01-15\n" +
		"20240101000002,987654321,TEST LLC,TEST PLAN,50,2024-02-20\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.form_5500", testMainCols, 1)

	ds := &Form5500{}
	spec := form5500Specs[zipMainForm]
	rows, err := ds.parseCSVDynamic(context.Background(), pool, strings.NewReader(csvContent), spec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestForm5500_ProcessZip_InvalidZipFile(t *testing.T) {
	dir := t.TempDir()
	badZip := filepath.Join(dir, "bad.zip")
	require.NoError(t, os.WriteFile(badZip, []byte("not a zip"), 0644))

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &Form5500{}
	_, err = ds.processZip(context.Background(), pool, badZip, zipMainForm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open zip")
}

func TestForm5500_ProcessZip_UnknownZipType(t *testing.T) {
	dir := t.TempDir()
	zipPath := createTestZip(t, dir, "test.zip", "test.csv", testMainCSVHeader+"row,data\n")

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &Form5500{}
	_, err = ds.processZip(context.Background(), pool, zipPath, form5500ZipType(99))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown zip type")
}

func TestForm5500_ColumnCounts(t *testing.T) {
	// Sanity check: verify we have the expected number of valid columns per table.
	assert.Equal(t, 140, len(form5500Specs[zipMainForm].validCols), "main form columns")
	assert.Equal(t, 191, len(form5500Specs[zipShortForm].validCols), "short form columns")
	assert.Equal(t, 166, len(form5500Specs[zipScheduleH].validCols), "schedule H columns")
	assert.Equal(t, 15, len(form5500Specs[zipScheduleC].validCols), "schedule C columns")
}
