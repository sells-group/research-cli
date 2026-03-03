package dataset

import (
	"context"
	"io"
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

func TestSBA7a504_Metadata(t *testing.T) {
	ds := &SBA7a504{}
	assert.Equal(t, "sba_7a_504", ds.Name())
	assert.Equal(t, "fed_data.sba_loans", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Quarterly, ds.Cadence())
}

func TestSBA7a504_ShouldRun(t *testing.T) {
	ds := &SBA7a504{}

	// Never synced -> should run
	now := time.Date(2024, time.May, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced recently (after quarter data became available) -> should not run
	lastSync := time.Date(2024, time.May, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &lastSync))

	// Synced before the latest quarter availability -> should run
	oldSync := time.Date(2023, time.December, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &oldSync))
}

var sbaCols = sbaColumns()

const sba7aCSVHeader = "AsOfDate,Program,L2LocID,BorrName,BorrStreet,BorrCity,BorrState,BorrZip,BankName,BankFDICNumber,BankNCUANumber,BankStreet,BankCity,BankState,BankZip,GrossApproval,SBAGuaranteedApproval,ApprovalDate,ApprovalFiscalYear,FirstDisbursementDate,ProcessingMethod,SubProgram,InitialInterestRate,FixedOrVariableInterestInd,TermInMonths,NaicsCode,NaicsDescription,FranchiseCode,FranchiseName,ProjectCounty,ProjectState,SBADistrictOffice,CongressionalDistrict,BusinessType,BusinessAge,LoanStatus,PaidInFullDate,ChargeOffDate,GrossChargeOffAmount,RevolverStatus,JobsSupported,CollateralInd,SoldSecMrktInd\n"

const sba504CSVHeader = "AsOfDate,Program,L2LocID,BorrName,BorrStreet,BorrCity,BorrState,BorrZip,CDC_Name,CDC_Street,CDC_City,CDC_State,CDC_Zip,ThirdPartyLender_Name,ThirdPartyLender_City,ThirdPartyLender_State,ThirdPartyDollars,GrossApproval,ApprovalDate,ApprovalFiscalYear,FirstDisbursementDate,ProcessingMethod,DeliveryMethod,SubProgram,TermInMonths,NaicsCode,NaicsDescription,FranchiseCode,FranchiseName,ProjectCounty,ProjectState,SBADistrictOffice,CongressionalDistrict,BusinessType,BusinessAge,LoanStatus,PaidInFullDate,ChargeOffDate,GrossChargeOffAmount,JobsSupported,CollateralInd\n"

func TestSBA7a504_ParseCSV_7A(t *testing.T) {
	csvContent := sba7aCSVHeader +
		"09/30/2024,7A,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing or more than 2 years old,EXEMPT,,,0.00,0,10,Y,N\n" +
		"09/30/2024,7A,1000002,SMITH CONSULTING INC,456 Oak Ave,Denver,CO,80202,ROCKY MOUNTAIN BANK,67890,,200 Finance Blvd,Denver,CO,80203,150000.00,112500.00,06/20/2023,2023,07/15/2023,PLP,Express,8.25,V,84,523110,Investment Banking,00000,,Denver,CO,0801,CO-01,Individual,New Business,PIF,01/15/2025,,0.00,0,5,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 2)

	ds := &SBA7a504{}
	rows, n7a, n504, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, int64(2), n7a)
	assert.Equal(t, int64(0), n504)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_ParseCSV_504(t *testing.T) {
	csvContent := sba504CSVHeader +
		"09/30/2024,504,2000001,PACIFIC PROPERTIES LLC,100 Beach Rd,San Diego,CA,92101,CALIFORNIA CDC,50 CDC Way,Los Angeles,CA,90001,PACIFIC FIRST BANK,San Diego,CA,800000.00,400000.00,05/20/2023,2023,06/15/2023,Regular,504,Standard 504,240,531120,Lessors of Nonresidential Buildings,00000,,San Diego,CA,0901,CA-52,LLC,Existing or more than 2 years old,EXEMPT,,,0.00,15,Y\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)

	ds := &SBA7a504{}
	rows, n7a, n504, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "504")
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, int64(0), n7a)
	assert.Equal(t, int64(1), n504)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_ParseCSV_SkipsInvalidL2LocID(t *testing.T) {
	csvContent := sba7aCSVHeader +
		"09/30/2024,7A,,INVALID CORP,123 Main St,Austin,TX,78701,BANK,12345,,100 Bank Ave,Dallas,TX,75201,100000,75000,03/15/2023,2023,04/01/2023,Regular,Standard,7.50,V,120,541511,Desc,00000,,Travis,TX,0504,TX-25,Corp,Existing,Active,,,0.00,0,10,Y,N\n" +
		"09/30/2024,7A,1000001,VALID CORP,456 Oak Ave,Dallas,TX,75201,BANK,67890,,200 Bank Ave,Dallas,TX,75201,50000,37500,06/20/2023,2023,07/15/2023,PLP,Express,8.25,V,84,523110,Desc,00000,,Dallas,TX,0801,TX-30,LLC,New,Active,,,0.00,0,5,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)

	ds := &SBA7a504{}
	rows, _, _, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_ParseCSV_EmptyCSV(t *testing.T) {
	csvContent := sba7aCSVHeader

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &SBA7a504{}
	rows, _, _, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestSBA7a504_ParseCSV_DateParsing(t *testing.T) {
	// Verify that MM/DD/YYYY dates are parsed correctly.
	d := parseSBADate("03/15/2023")
	require.NotNil(t, d)
	assert.Equal(t, 2023, d.Year())
	assert.Equal(t, time.March, d.Month())
	assert.Equal(t, 15, d.Day())

	// Verify YYYY-MM-DD format.
	d2 := parseSBADate("2023-03-15")
	require.NotNil(t, d2)
	assert.Equal(t, 2023, d2.Year())

	// Empty returns nil.
	assert.Nil(t, parseSBADate(""))
	assert.Nil(t, parseSBADate("  "))
	assert.Nil(t, parseSBADate("invalid"))
}

func TestSBA7a504_DetectProgram(t *testing.T) {
	assert.Equal(t, "7A", detectProgram("FOIA - 7(a)(FY2010-Present)"))
	assert.Equal(t, "7A", detectProgram("FOIA - 7(a) (FY1991-FY2009)"))
	assert.Equal(t, "504", detectProgram("FOIA - 504 (FY1991-Present)"))
	assert.Equal(t, "", detectProgram("SBA Data Dictionary.xlsx"))
	assert.Equal(t, "", detectProgram("README"))
}

func TestSBA7a504_Sync_DiscoverError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, assert.AnError)

	ds := &SBA7a504{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CKAN metadata")
}

func TestSBA7a504_Sync_NoCSVResources(t *testing.T) {
	ckanJSON := `{"result":{"resources":[{"name":"readme.pdf","url":"https://example.com/readme.pdf","format":"PDF"}]}}`

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	ds := &SBA7a504{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV resources")
}

func TestSBA7a504_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	ckanJSON := `{"result":{"resources":[{"name":"FOIA - 7(a)(FY2010-Present).csv","url":"https://example.com/7a.csv","format":"CSV"}]}}`

	csvContent := sba7aCSVHeader +
		"09/30/2024,7A,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing,EXEMPT,,,0.00,0,10,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Mock CKAN API call.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "api/3/action/package_show")
	})).Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	// Mock CSV download.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			if writeErr := os.WriteFile(destPath, []byte(csvContent), 0644); writeErr != nil {
				panic("test: write CSV: " + writeErr.Error())
			}
		}).Return(int64(len(csvContent)), nil)

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)

	ds := &SBA7a504{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.Equal(t, int64(1), result.Metadata["rows_7a"])
	assert.Equal(t, int64(0), result.Metadata["rows_504"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_Sync_DownloadError(t *testing.T) {
	ckanJSON := `{"result":{"resources":[{"name":"FOIA - 7(a).csv","url":"https://example.com/7a.csv","format":"CSV"}]}}`

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &SBA7a504{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestSBA7a504_NilIfEmpty(t *testing.T) {
	assert.Nil(t, nilIfEmpty(""))
	v := nilIfEmpty("test")
	require.NotNil(t, v)
	assert.Equal(t, "test", v)
}

func TestSBA7a504_ParseNullFloat(t *testing.T) {
	assert.Nil(t, parseNullFloat(""))
	assert.Nil(t, parseNullFloat("abc"))

	v := parseNullFloat("250000.00")
	require.NotNil(t, v)
	assert.InDelta(t, 250000.0, *v, 0.01)

	z := parseNullFloat("0")
	require.NotNil(t, z)
	assert.InDelta(t, 0.0, *z, 0.01)
}

func TestSBA7a504_ParseNullInt(t *testing.T) {
	assert.Nil(t, parseNullInt(""))
	assert.Nil(t, parseNullInt("abc"))

	v := parseNullInt("120")
	require.NotNil(t, v)
	assert.Equal(t, 120, *v)

	z := parseNullInt("0")
	require.NotNil(t, z)
	assert.Equal(t, 0, *z)
}

func TestSBA7a504_Sync_InvalidCKANJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader("not valid json")), nil)

	ds := &SBA7a504{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode CKAN metadata")
}

func TestSBA7a504_ProcessCSV_FileNotFound(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &SBA7a504{}
	_, _, _, err = ds.processCSV(context.Background(), pool, "/nonexistent/path.csv", "7A")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open CSV")
}

func TestSBA7a504_ParseCSV_HeaderError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &SBA7a504{}
	// Empty reader -> EOF on header read.
	rows, _, _, err := ds.parseCSV(context.Background(), pool, strings.NewReader(""), "7A")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read CSV header")
	assert.Equal(t, int64(0), rows)
}

func TestSBA7a504_ParseCSV_BatchFinalFlush(t *testing.T) {
	// Ensure the final batch flush path works with a small number of rows
	// (less than sbaBatchSize).
	csvContent := sba7aCSVHeader +
		"09/30/2024,7A,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing,EXEMPT,,,0.00,0,10,Y,N\n" +
		"09/30/2024,7A,1000002,BETA CORP,789 Elm St,Denver,CO,80202,ROCKY MTN BANK,67890,,200 Bank Blvd,Denver,CO,80203,100000.00,75000.00,06/20/2023,2023,07/15/2023,PLP,Express,8.25,V,84,523110,Investment Banking,00000,,Denver,CO,0801,CO-01,LLC,New Business,PIF,01/15/2025,,0.00,0,5,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 2)

	ds := &SBA7a504{}
	rows, n7a, n504, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, int64(2), n7a)
	assert.Equal(t, int64(0), n504)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_ParseCSV_ProgramFallback(t *testing.T) {
	// CSV with empty program column — should fall back to defaultProgram.
	csvContent := sba7aCSVHeader +
		"09/30/2024,,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing,EXEMPT,,,0.00,0,10,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)

	ds := &SBA7a504{}
	rows, n7a, _, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, int64(1), n7a)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestSBA7a504_ParseCSV_InvalidProgram(t *testing.T) {
	// CSV with an unrecognized program value — should skip the row.
	csvContent := sba7aCSVHeader +
		"09/30/2024,UNKNOWN,1000001,ACME LLC,123 Main St,Austin,TX,78701,BANK,12345,,100 Bank Ave,Dallas,TX,75201,100000,75000,03/15/2023,2023,04/01/2023,Regular,Standard,7.50,V,120,541511,Desc,00000,,Travis,TX,0504,TX-25,Corp,Existing,Active,,,0.00,0,10,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &SBA7a504{}
	rows, _, _, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "INVALID")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows) // skipped because program is neither 7A nor 504
}

func TestSBA7a504_ParseCSV_UpsertError(t *testing.T) {
	csvContent := sba7aCSVHeader +
		"09/30/2024,7A,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing,EXEMPT,,,0.00,0,10,Y,N\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Make the CREATE TEMP TABLE fail to trigger the upsert error path.
	pool.ExpectExec("CREATE TEMP TABLE").WillReturnError(assert.AnError)

	ds := &SBA7a504{}
	_, _, _, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent), "7A")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bulk upsert")
}

func TestSBA7a504_Sync_MultipleResources(t *testing.T) {
	dir := t.TempDir()

	ckanJSON := `{"result":{"resources":[
		{"name":"FOIA - 7(a)(FY2010-Present).csv","url":"https://example.com/7a.csv","format":"CSV"},
		{"name":"FOIA - 504 (FY2010-Present).csv","url":"https://example.com/504.csv","format":"CSV"},
		{"name":"Data Dictionary.xlsx","url":"https://example.com/dict.xlsx","format":"XLSX"}
	]}}`

	csv7a := sba7aCSVHeader +
		"09/30/2024,7A,1000001,ACME WIDGETS LLC,123 Main St,Austin,TX,78701,FIRST NATIONAL BANK,12345,,100 Bank Ave,Dallas,TX,75201,250000.00,187500.00,03/15/2023,2023,04/01/2023,Regular,Standard 7(a),7.50,V,120,541511,Custom Computer Programming Services,00000,,Travis,TX,0504,TX-25,Corporation,Existing,EXEMPT,,,0.00,0,10,Y,N\n"

	csv504 := sba504CSVHeader +
		"09/30/2024,504,2000001,PACIFIC PROPERTIES LLC,100 Beach Rd,San Diego,CA,92101,CALIFORNIA CDC,50 CDC Way,Los Angeles,CA,90001,PACIFIC FIRST BANK,San Diego,CA,800000.00,400000.00,05/20/2023,2023,06/15/2023,Regular,504,Standard 504,240,531120,Lessors of Nonresidential Buildings,00000,,San Diego,CA,0901,CA-52,LLC,Existing or more than 2 years old,EXEMPT,,,0.00,15,Y\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "api/3/action/package_show")
	})).Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	// Mock CSV downloads — use Run to write different content per file.
	f.EXPECT().DownloadToFile(mock.Anything, "https://example.com/7a.csv", mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			if writeErr := os.WriteFile(destPath, []byte(csv7a), 0644); writeErr != nil {
				panic("test: write CSV: " + writeErr.Error())
			}
		}).Return(int64(len(csv7a)), nil)

	f.EXPECT().DownloadToFile(mock.Anything, "https://example.com/504.csv", mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			if writeErr := os.WriteFile(destPath, []byte(csv504), 0644); writeErr != nil {
				panic("test: write CSV: " + writeErr.Error())
			}
		}).Return(int64(len(csv504)), nil)

	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)
	expectBulkUpsertZip(pool, "fed_data.sba_loans", sbaCols, 1)

	ds := &SBA7a504{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.Equal(t, int64(1), result.Metadata["rows_7a"])
	assert.Equal(t, int64(1), result.Metadata["rows_504"])
	assert.Equal(t, 2, result.Metadata["files"])
	assert.NoError(t, pool.ExpectationsWereMet())
}
