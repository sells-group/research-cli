package dataset

import (
	"context"
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

func TestNCEN_Metadata(t *testing.T) {
	ds := &NCEN{}
	assert.Equal(t, "ncen", ds.Name())
	assert.Equal(t, "fed_data.ncen_registrants", ds.Table())
	assert.Equal(t, Phase2, ds.Phase())
	assert.Equal(t, Quarterly, ds.Cadence())
}

func TestNCEN_ShouldRun(t *testing.T) {
	ds := &NCEN{}

	// Never synced -> should run.
	now := time.Date(2025, time.September, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced recently within the quarter -> should not run.
	recent := time.Date(2025, time.August, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &recent))

	// Synced last quarter -> should run.
	lastQ := time.Date(2025, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastQ))
}

func TestNCEN_ParseTSV(t *testing.T) {
	input := "COL_A\tCOL_B\tCOL_C\n" +
		"val1\tval2\tval3\n" +
		"val4\tval5\tval6\n"

	result, err := parseTSV(strings.NewReader(input))
	require.NoError(t, err)
	assert.Len(t, result.records, 2)
	assert.Equal(t, 0, result.colIdx["col_a"])
	assert.Equal(t, 1, result.colIdx["col_b"])
	assert.Equal(t, 2, result.colIdx["col_c"])
	assert.Equal(t, "val1", result.records[0][0])
	assert.Equal(t, "val6", result.records[1][2])
}

func TestNCEN_ParseTSV_EmptyFile(t *testing.T) {
	_, err := parseTSV(strings.NewReader(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read TSV header")
}

func TestNCEN_ParseNCENDate(t *testing.T) {
	// DD-MON-YYYY format.
	d := parseNCENDate("15-JUL-2025")
	require.NotNil(t, d)
	assert.Equal(t, time.Date(2025, time.July, 15, 0, 0, 0, 0, time.UTC), d)

	// ISO format.
	d = parseNCENDate("2025-03-31")
	require.NotNil(t, d)
	assert.Equal(t, time.Date(2025, time.March, 31, 0, 0, 0, 0, time.UTC), d)

	// Empty.
	assert.Nil(t, parseNCENDate(""))

	// Unparseable.
	assert.Nil(t, parseNCENDate("not-a-date"))
}

func TestNCEN_ParseBoolYNOrNil(t *testing.T) {
	assert.Equal(t, true, parseBoolYNOrNil("Y"))
	assert.Equal(t, true, parseBoolYNOrNil("y"))
	assert.Equal(t, false, parseBoolYNOrNil("N"))
	assert.Equal(t, false, parseBoolYNOrNil("n"))
	assert.Nil(t, parseBoolYNOrNil(""))
	assert.Nil(t, parseBoolYNOrNil("true"))
}

func TestNCEN_BuildRegistrantRows(t *testing.T) {
	subData, err := os.ReadFile("testdata/ncen_submission.tsv")
	require.NoError(t, err)
	regData, err := os.ReadFile("testdata/ncen_registrant.tsv")
	require.NoError(t, err)

	sub, err := parseTSV(strings.NewReader(string(subData)))
	require.NoError(t, err)
	reg, err := parseTSV(strings.NewReader(string(regData)))
	require.NoError(t, err)

	ds := &NCEN{}
	now := time.Date(2025, time.October, 1, 0, 0, 0, 0, time.UTC)
	rows := ds.buildRegistrantRows(sub, reg, now)

	assert.Len(t, rows, 3)

	// Verify first row (ACME).
	r := rows[0]
	assert.Equal(t, "0000111111-25-000001", r[0]) // accession_number
	assert.Equal(t, "0000111111", r[1])           // cik
	assert.Equal(t, "ACME FUNDS INC", r[2])       // registrant_name
	assert.Equal(t, "811-00001", r[3])            // file_num
	assert.Equal(t, "549300AAAA1111111111", r[4]) // lei
	assert.Equal(t, "100 Main Street", r[5])      // address1
	assert.Equal(t, "Suite 200", r[6])            // address2
	assert.Equal(t, "New York", r[7])             // city
	assert.Equal(t, "US-NY", r[8])                // state
	assert.Equal(t, "US", r[9])                   // country
	assert.Equal(t, "10001", r[10])               // zip
	assert.Equal(t, "212-555-0100", r[11])        // phone
	assert.Equal(t, "N-1A", r[12])                // investment_company_type
	assert.Equal(t, int64(5), r[13])              // total_series
	assert.NotNil(t, r[14])                       // filing_date
	assert.NotNil(t, r[15])                       // report_ending_period
	assert.Equal(t, false, r[16])                 // is_first_filing
	assert.Equal(t, false, r[17])                 // is_last_filing
	assert.Equal(t, "ACME", r[18])                // family_investment_company_name
	assert.Equal(t, now, r[19])                   // updated_at

	// Verify third row (GAMMA) — is_first_filing=Y, no LEI.
	r = rows[2]
	assert.Equal(t, "0000333333-25-000003", r[0])
	assert.Equal(t, "0000333333", r[1])
	assert.Nil(t, r[4])          // lei is empty
	assert.Equal(t, true, r[16]) // is_first_filing = Y
	assert.Equal(t, "N-2", r[12])
}

func TestNCEN_BuildFundRows(t *testing.T) {
	data, err := os.ReadFile("testdata/ncen_fund_reported_info.tsv")
	require.NoError(t, err)

	parsed, err := parseTSV(strings.NewReader(string(data)))
	require.NoError(t, err)

	ds := &NCEN{}
	now := time.Date(2025, time.October, 1, 0, 0, 0, 0, time.UTC)
	rows := ds.buildFundRows(parsed, now)

	assert.Len(t, rows, 4)

	// Verify first fund (Acme Growth Fund).
	r := rows[0]
	assert.Equal(t, "0000111111-25-000001_S000001001", r[0]) // fund_id
	assert.Equal(t, "0000111111-25-000001", r[1])            // accession_number
	assert.Equal(t, "Acme Growth Fund", r[2])                // fund_name
	assert.Equal(t, "S000001001", r[3])                      // series_id
	assert.Equal(t, "549300FUND0001000001", r[4])            // lei
	assert.Equal(t, false, r[5])                             // is_etf (N)
	assert.Equal(t, false, r[6])                             // is_index (N)
	assert.Equal(t, false, r[7])                             // is_money_market
	assert.Equal(t, false, r[8])                             // is_target_date
	assert.Equal(t, false, r[9])                             // is_fund_of_fund
	assert.InDelta(t, 5000000000.50, r[10], 0.01)            // monthly_avg_net_assets
	assert.Nil(t, r[11])                                     // daily_avg_net_assets (empty)
	assert.InDelta(t, 25.50, r[12], 0.01)                    // nav_per_share
	assert.InDelta(t, 0.75, r[13], 0.01)                     // management_fee
	assert.InDelta(t, 0.95, r[14], 0.01)                     // net_operating_expenses
	assert.Equal(t, now, r[15])                              // updated_at

	// Verify ETF fund (Beta S&P 500 ETF).
	r = rows[2]
	assert.Equal(t, true, r[5])                    // is_etf (Y)
	assert.Equal(t, true, r[6])                    // is_index (Y)
	assert.InDelta(t, 10000000000.00, r[10], 0.01) // monthly_avg_net_assets
}

func TestNCEN_BuildAdviserRows(t *testing.T) {
	data, err := os.ReadFile("testdata/ncen_adviser.tsv")
	require.NoError(t, err)

	parsed, err := parseTSV(strings.NewReader(string(data)))
	require.NoError(t, err)

	ds := &NCEN{}
	now := time.Date(2025, time.October, 1, 0, 0, 0, 0, time.UTC)
	rows := ds.buildAdviserRows(parsed, now)

	assert.Len(t, rows, 4)

	// Verify first adviser.
	r := rows[0]
	assert.Equal(t, "0000111111-25-000001_S000001001", r[0]) // fund_id
	assert.Equal(t, "Acme Capital Management LLC", r[1])     // adviser_name
	assert.Equal(t, "000100001", r[2])                       // adviser_crd
	assert.Equal(t, "549300ADV00001000001", r[3])            // adviser_lei
	assert.Equal(t, "801-11111", r[4])                       // file_num
	assert.Equal(t, "Advisor", r[5])                         // adviser_type
	assert.Equal(t, "US-NY", r[6])                           // state
	assert.Equal(t, "US", r[7])                              // country
	assert.Nil(t, r[8])                                      // is_affiliated (empty)
	assert.Equal(t, now, r[9])                               // updated_at

	// Verify affiliated adviser.
	r = rows[2]
	assert.Equal(t, true, r[8]) // is_affiliated (Y)

	// Verify sub-adviser with no LEI.
	r = rows[3]
	assert.Equal(t, "Sub-Advisor", r[5])
	assert.Nil(t, r[3])          // adviser_lei is empty
	assert.Equal(t, false, r[8]) // is_affiliated (N)
}

func TestNCEN_QuarterHelpers(t *testing.T) {
	q := currentQuarter(time.Date(2025, time.September, 15, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, 2025, q.Year)
	assert.Equal(t, 3, q.Quarter)

	q = currentQuarter(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, 2025, q.Year)
	assert.Equal(t, 1, q.Quarter)

	// prevQuarter wraps year.
	q = prevQuarter(ncenQuarter{Year: 2025, Quarter: 1})
	assert.Equal(t, 2024, q.Year)
	assert.Equal(t, 4, q.Quarter)

	q = prevQuarter(ncenQuarter{Year: 2025, Quarter: 3})
	assert.Equal(t, 2025, q.Year)
	assert.Equal(t, 2, q.Quarter)

	// String.
	assert.Equal(t, "2025q3", ncenQuarter{Year: 2025, Quarter: 3}.String())
}

func TestNCEN_UpsertConfigs(t *testing.T) {
	cfg := ncenRegistrantUpsertCfg()
	assert.Equal(t, "fed_data.ncen_registrants", cfg.Table)
	assert.Equal(t, []string{"accession_number"}, cfg.ConflictKeys)
	assert.Equal(t, ncenRegistrantCols, cfg.Columns)

	cfg = ncenFundUpsertCfg()
	assert.Equal(t, "fed_data.ncen_funds", cfg.Table)
	assert.Equal(t, []string{"fund_id"}, cfg.ConflictKeys)
	assert.Equal(t, ncenFundCols, cfg.Columns)

	cfg = ncenAdviserUpsertCfg()
	assert.Equal(t, "fed_data.ncen_advisers", cfg.Table)
	assert.Equal(t, []string{"fund_id", "adviser_name", "adviser_type"}, cfg.ConflictKeys)
	assert.Equal(t, ncenAdviserCols, cfg.Columns)
}

func TestNCEN_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	subContent, err := os.ReadFile("testdata/ncen_submission.tsv")
	require.NoError(t, err)
	regContent, err := os.ReadFile("testdata/ncen_registrant.tsv")
	require.NoError(t, err)
	fundContent, err := os.ReadFile("testdata/ncen_fund_reported_info.tsv")
	require.NoError(t, err)
	advContent, err := os.ReadFile("testdata/ncen_adviser.tsv")
	require.NoError(t, err)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Mock download for each quarter — create a ZIP with all 4 TSV files.
	files := map[string]string{
		"SUBMISSION.tsv":         string(subContent),
		"REGISTRANT.tsv":         string(regContent),
		"FUND_REPORTED_INFO.tsv": string(fundContent),
		"ADVISER.tsv":            string(advContent),
	}

	// NCEN syncs ncenBackfillQuarters (8) quarters. For each quarter it tries
	// primary URL first. We return the same ZIP for all.
	for i := 0; i < ncenBackfillQuarters; i++ {
		f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
				createTestZipMulti(t, dir, fmt.Sprintf("q%d.zip", i), files)
				// Copy from the test zip to the requested path.
				zipPath := createTestZipMulti(t, dir, fmt.Sprintf("dl%d.zip", i), files)
				data, err := os.ReadFile(zipPath)
				if err != nil {
					return 0, err
				}
				return int64(len(data)), os.WriteFile(path, data, 0644)
			}).Once()
	}

	// Expect BulkUpsertMulti for each quarter: Begin, 3x(CREATE TEMP TABLE, COPY, DELETE, INSERT), Commit.
	regTemp := pgx.Identifier{"_tmp_upsert_fed_data_ncen_registrants"}
	fundTemp := pgx.Identifier{"_tmp_upsert_fed_data_ncen_funds"}
	advTemp := pgx.Identifier{"_tmp_upsert_fed_data_ncen_advisers"}

	for i := 0; i < ncenBackfillQuarters; i++ {
		pool.ExpectBegin()
		// registrants
		pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
		pool.ExpectCopyFrom(regTemp, ncenRegistrantCols).WillReturnResult(3)
		pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
		pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 3))
		// funds
		pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
		pool.ExpectCopyFrom(fundTemp, ncenFundCols).WillReturnResult(4)
		pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
		pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 4))
		// advisers
		pool.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
		pool.ExpectCopyFrom(advTemp, ncenAdviserCols).WillReturnResult(4)
		pool.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
		pool.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 4))
		pool.ExpectCommit()
	}

	ds := &NCEN{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)

	// 8 quarters × (3 registrants + 4 funds + 4 advisers) = 88
	assert.Equal(t, int64(ncenBackfillQuarters*(3+4+4)), result.RowsSynced)
	assert.Equal(t, int64(ncenBackfillQuarters*3), result.Metadata["registrants"])
	assert.Equal(t, int64(ncenBackfillQuarters*4), result.Metadata["funds"])
	assert.Equal(t, int64(ncenBackfillQuarters*4), result.Metadata["advisers"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestNCEN_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Both primary and fallback URLs fail (non-404 error).
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError).Times(2)

	ds := &NCEN{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ncen: sync quarter")
}

func TestNCEN_Sync_404SkipsQuarter(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// All downloads return 404.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), fmt.Errorf("HTTP 404 not found")).Maybe()

	ds := &NCEN{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)

	// All quarters skipped → 0 rows.
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestNCEN_BuildFundRows_EmptyFundID(t *testing.T) {
	ds := &NCEN{}
	now := time.Now()

	// Records where FUND_ID is empty should be skipped.
	data := &parsedTSV{
		colIdx:  map[string]int{"fund_id": 0, "accession_number": 1, "fund_name": 2},
		records: [][]string{{"", "acc-1", "Fund A"}, {"", "acc-2", "Fund B"}},
	}
	rows := ds.buildFundRows(data, now)
	assert.Empty(t, rows)
}

func TestNCEN_BuildAdviserRows_EmptyFundID(t *testing.T) {
	ds := &NCEN{}
	now := time.Now()

	// Records where FUND_ID is empty should be skipped.
	data := &parsedTSV{
		colIdx:  map[string]int{"fund_id": 0, "crd_num": 1, "adviser_name": 2, "adviser_type": 3},
		records: [][]string{{"", "123", "Adviser A", "Advisor"}},
	}
	rows := ds.buildAdviserRows(data, now)
	assert.Empty(t, rows)
}

func TestNCEN_ParseTSVFile_MissingFile(t *testing.T) {
	ds := &NCEN{}
	// File not in map → returns empty parsedTSV (not an error).
	result, err := ds.parseTSVFile(map[string]string{}, "MISSING.TSV")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.records)
	assert.NotNil(t, result.colIdx)
}

func TestNCEN_BuildRegistrantRows_NoMatchingRegistrant(t *testing.T) {
	ds := &NCEN{}
	now := time.Now()

	// Submission record with no matching registrant.
	sub := &parsedTSV{
		colIdx:  map[string]int{"accession_number": 0, "cik": 1, "filing_date": 2, "report_ending_period": 3},
		records: [][]string{{"acc-orphan", "0001234567", "2025-01-15", "2024-12-31"}},
	}
	reg := &parsedTSV{
		colIdx:  map[string]int{"accession_number": 0, "registrant_name": 1},
		records: [][]string{{"acc-other", "Some Other Fund"}},
	}

	rows := ds.buildRegistrantRows(sub, reg, now)
	assert.Len(t, rows, 1)

	r := rows[0]
	assert.Equal(t, "acc-orphan", r[0]) // accession_number
	assert.Equal(t, "0001234567", r[1]) // cik
	assert.Nil(t, r[2])                 // registrant_name (nil — no matching reg)
	assert.NotNil(t, r[14])             // filing_date still parsed
}

func TestNCEN_BuildRegistrantRows_EmptyAccession(t *testing.T) {
	ds := &NCEN{}
	now := time.Now()

	sub := &parsedTSV{
		colIdx:  map[string]int{"accession_number": 0, "cik": 1},
		records: [][]string{{"", "0001234567"}},
	}
	reg := &parsedTSV{
		colIdx:  map[string]int{"accession_number": 0},
		records: nil,
	}

	rows := ds.buildRegistrantRows(sub, reg, now)
	assert.Empty(t, rows)
}

func TestNCEN_Sync_ZIPExtractError(t *testing.T) {
	dir := t.TempDir()
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Download writes a non-ZIP file.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return 4, os.WriteFile(path, []byte("nope"), 0644)
		}).Once()

	ds := &NCEN{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ncen: sync quarter")
}

func TestNCEN_ParseTSVFile_OpenError(t *testing.T) {
	ds := &NCEN{}
	// File in map but path doesn't exist → os.Open error.
	result, err := ds.parseTSVFile(map[string]string{"TEST.TSV": "/nonexistent/path.tsv"}, "TEST.TSV")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "open TEST.TSV")
}

func TestNCEN_DownloadQuarter_FallbackURL(t *testing.T) {
	dir := t.TempDir()

	f := fetchermocks.NewMockFetcher(t)

	q := ncenQuarter{Year: 2024, Quarter: 1}

	// Primary URL fails.
	primaryURL := fmt.Sprintf("%s/2024q1_ncen.zip", ncenBaseURL)
	f.EXPECT().DownloadToFile(mock.Anything, primaryURL, mock.Anything).
		Return(int64(0), assert.AnError).Once()

	// Fallback URL succeeds.
	altURL := fmt.Sprintf("%s/2024q1_ncen_0.zip", ncenBaseURL)
	f.EXPECT().DownloadToFile(mock.Anything, altURL, mock.Anything).
		Return(int64(100), nil).Once()

	ds := &NCEN{}
	err := ds.downloadQuarter(context.Background(), f, q, filepath.Join(dir, "test.zip"))
	assert.NoError(t, err)
}
