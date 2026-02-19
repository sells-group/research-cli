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

func TestPPP_Metadata(t *testing.T) {
	ds := &PPP{}
	assert.Equal(t, "ppp", ds.Name())
	assert.Equal(t, "fed_data.ppp_loans", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestPPP_ShouldRun(t *testing.T) {
	ds := &PPP{}
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)

	// Never synced -> should run
	assert.True(t, ds.ShouldRun(now, nil))

	// Already synced -> should not run (one-time load)
	lastSync := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &lastSync))
}

var pppCols = []string{
	"loannumber", "borrowername", "borroweraddress", "borrowercity",
	"borrowerstate", "borrowerzip", "currentapprovalamount", "forgivenessamount",
	"jobsreported", "dateapproved", "loanstatus", "businesstype",
	"naicscode", "businessagedescription",
}

const pppCSVHeader = "LoanNumber,BorrowerName,BorrowerAddress,BorrowerCity,BorrowerState,BorrowerZip,CurrentApprovalAmount,ForgivenessAmount,JobsReported,DateApproved,LoanStatus,BusinessType,NAICSCode,BusinessAgeDescription\n"

func TestPPP_ParseCSV_Success(t *testing.T) {
	csvContent := pppCSVHeader +
		"1234567,ACME CORP,123 Main St,Austin,TX,78701,150000.00,150000.00,15,04/03/2020,Paid in Full,Corporation,541511,Existing or more than 2 years old\n" +
		"7654321,TEST LLC,456 Oak Ave,Dallas,TX,75201,75000.50,75000.50,5,05/15/2020,Paid in Full,LLC,523110,New Business or 2 years or less\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.ppp_loans", pppCols, 2)

	ds := &PPP{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestPPP_ParseCSV_SkipsInvalidLoanNumber(t *testing.T) {
	csvContent := pppCSVHeader +
		",INVALID CORP,123 Main St,Austin,TX,78701,100000,100000,10,04/03/2020,Active,Corporation,541511,Existing\n" +
		"1234567,VALID CORP,456 Oak Ave,Dallas,TX,75201,50000,50000,5,05/15/2020,Active,LLC,523110,New\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsertZip(pool, "fed_data.ppp_loans", pppCols, 1)

	ds := &PPP{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestPPP_ParseCSV_EmptyCSV(t *testing.T) {
	csvContent := pppCSVHeader

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &PPP{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestPPP_Sync_DiscoverError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, assert.AnError)

	ds := &PPP{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CKAN metadata")
}

func TestPPP_Sync_NoCSVResources(t *testing.T) {
	ckanJSON := `{"result":{"resources":[{"name":"readme.pdf","url":"https://example.com/readme.pdf","format":"PDF"}]}}`

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	ds := &PPP{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV resources")
}

func TestPPP_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	ckanJSON := `{"result":{"resources":[{"name":"public_150k_plus.csv","url":"https://example.com/ppp.csv","format":"CSV"}]}}`

	csvContent := pppCSVHeader +
		"1234567,ACME CORP,123 Main St,Austin,TX,78701,150000,150000,15,04/03/2020,Active,Corporation,541511,Existing\n"

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
			if err := os.WriteFile(destPath, []byte(csvContent), 0644); err != nil {
				panic("test: write CSV: " + err.Error())
			}
		}).Return(int64(len(csvContent)), nil)

	expectBulkUpsertZip(pool, "fed_data.ppp_loans", pppCols, 1)

	ds := &PPP{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestPPP_Sync_DownloadError(t *testing.T) {
	ckanJSON := `{"result":{"resources":[{"name":"test.csv","url":"https://example.com/test.csv","format":"CSV"}]}}`

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader(ckanJSON)), nil)

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &PPP{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}
