package dataset

import (
	"context"
	"fmt"
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

func TestEOBMF_Metadata(t *testing.T) {
	ds := &EOBMF{}
	assert.Equal(t, "eo_bmf", ds.Name())
	assert.Equal(t, "fed_data.eo_bmf", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Monthly, ds.Cadence())
}

func TestEOBMF_ShouldRun(t *testing.T) {
	ds := &EOBMF{}

	// Never synced -> should run.
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced this month -> should not run.
	thisMonth := time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisMonth))

	// Synced last month -> should run.
	lastMonth := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastMonth))
}

const eoBMFCSVHeader = "EIN,NAME,ICO,STREET,CITY,STATE,ZIP,GROUP,SUBSECTION,AFFILIATION,CLASSIFICATION,RULING,DEDUCTIBILITY,FOUNDATION,ACTIVITY,ORGANIZATION,STATUS,TAX_PERIOD,ASSET_CD,INCOME_CD,FILING_REQ_CD,PF_FILING_REQ_CD,ACCT_PD,ASSET_AMT,INCOME_AMT,REVENUE_AMT,NTEE_CD,SORT_NAME\n"

func TestEOBMF_ParseCSV_Success(t *testing.T) {
	csvContent := eoBMFCSVHeader +
		"010202467,ALABAMA COUNCIL ON HUMAN RELATIONS INC,,PO BOX 409,AUBURN,AL,36831-0409,0000,03,3,1000,197103,1,15,0,1,01,202209,3,3,01,00,9,237027,22882,22882,S99,\n" +
		"010211478,GREENSBORO LODGE 1498,LOYAL ORDER OF MOOSE,PO BOX 97,GREENSBORO,AL,36744-0097,0000,08,3,4000,194109,2,09,0,2,01,201312,0,0,02,00,12,0,0,0,,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 2)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_SkipsEmptyEIN(t *testing.T) {
	csvContent := eoBMFCSVHeader +
		",MISSING EIN ORG,,123 MAIN ST,ANYTOWN,NY,10001,0000,03,3,1000,200001,1,15,0,1,01,202209,0,0,01,00,12,0,0,0,,\n" +
		"123456789,VALID ORG,,456 OAK AVE,DALLAS,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,5,5,01,00,12,500000,100000,100000,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 1)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_EmptyCSV(t *testing.T) {
	csvContent := eoBMFCSVHeader

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestEOBMF_ParseCSV_DuplicateEIN(t *testing.T) {
	// Same EIN appearing twice — BulkUpsert dedup keeps the last.
	csvContent := eoBMFCSVHeader +
		"123456789,FIRST VERSION,,100 MAIN ST,AUSTIN,TX,78701,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,100000,50000,50000,B11,\n" +
		"123456789,SECOND VERSION,,200 OAK AVE,DALLAS,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,5,5,01,00,12,500000,200000,200000,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 2)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_InvalidHeader(t *testing.T) {
	// Header with no recognized columns (no EIN column).
	csvContent := "BOGUS,COLUMNS,NOTHING\n1,2,3\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &EOBMF{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EIN column not found")
}

func TestEOBMF_ParseCSV_NonNumericFields(t *testing.T) {
	// Test mapRow with non-numeric values in numeric fields — should produce nil.
	csvContent := eoBMFCSVHeader +
		"999999999,TEST ORG,,100 MAIN,CITY,TX,75201,0000,abc,xyz,1000,200001,notnum,15,0,bad,01,202209,xx,yy,zz,ww,qq,notint,notint,notint,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 1)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_AllEmptyOptionalFields(t *testing.T) {
	// All optional fields are empty — should produce nil values.
	csvContent := eoBMFCSVHeader +
		"888888888,MINIMAL ORG,,,,,,,,,,,,,,,,,,,,,,,,,,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 1)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_UpsertError(t *testing.T) {
	csvContent := eoBMFCSVHeader +
		"123456789,TEST ORG,,100 MAIN,CITY,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,100000,50000,50000,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Make the BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &EOBMF{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eo_bmf: bulk upsert")
}

func TestEOBMF_ParseCSV_EmptyReader(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &EOBMF{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eo_bmf: read CSV header")
}

func TestEOBMF_ParseCSV_FinalBatchUpsertError(t *testing.T) {
	// Use fewer rows than batch size so it hits the final batch path.
	csvContent := eoBMFCSVHeader +
		"111111111,ORG ONE,,100 MAIN,CITY,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,100000,50000,50000,B11,\n" +
		"222222222,ORG TWO,,200 OAK,TOWN,CA,90001,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,200000,80000,80000,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Final batch upsert fails.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &EOBMF{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eo_bmf: bulk upsert final batch")
}

func TestEOBMF_ParseCSV_MalformedRow(t *testing.T) {
	// Row with wrong number of fields — csv.Reader returns an error, should be skipped.
	csvContent := eoBMFCSVHeader +
		"too,few,fields\n" +
		"123456789,VALID ORG,,456 OAK AVE,DALLAS,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,5,5,01,00,12,500000,100000,100000,B11,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 1)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_Sync_OpenError(t *testing.T) {
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Download succeeds but writes to a path, then we remove it before syncRegion calls os.Open.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			// Write then immediately delete so os.Open fails.
			_ = os.WriteFile(destPath, []byte("data"), 0644)
			_ = os.Remove(destPath)
		}).
		Return(int64(4), nil)

	ds := &EOBMF{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "eo_bmf: open")
}

func TestEOBMF_Sync_ParseError(t *testing.T) {
	dir := t.TempDir()

	// Write an invalid CSV file (no EIN column) that will cause parseCSV to error.
	badCSV := "BOGUS,COLUMNS\n1,2\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			writeTestFixture(t, destPath, []byte(badCSV))
		}).
		Return(int64(len(badCSV)), nil)

	ds := &EOBMF{}
	_, err = ds.Sync(context.Background(), pool, f, dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EIN column not found")
}

func TestEOBMF_ParseCSV_MidBatchUpsert(t *testing.T) {
	// Generate enough rows to trigger the mid-batch upsert path (batch >= 10000).
	var sb strings.Builder
	sb.WriteString(strings.TrimSuffix(eoBMFCSVHeader, "\n") + "\n")
	for i := range 10002 {
		fmt.Fprintf(&sb, "%09d,ORG %d,,100 MAIN,CITY,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,100000,50000,50000,B11,\n", 100000000+i, i)
	}

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// First batch of 10000 rows.
	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 10000)
	// Final batch of remaining 2 rows.
	expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 2)

	ds := &EOBMF{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(sb.String()))
	require.NoError(t, err)
	assert.Equal(t, int64(10002), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEOBMF_ParseCSV_MidBatchUpsertError(t *testing.T) {
	// Generate enough rows to trigger the mid-batch upsert, then fail it.
	var sb strings.Builder
	sb.WriteString(strings.TrimSuffix(eoBMFCSVHeader, "\n") + "\n")
	for i := range 10001 {
		fmt.Fprintf(&sb, "%09d,ORG %d,,100 MAIN,CITY,TX,75201,0000,03,3,1000,200001,1,15,0,1,01,202209,3,3,01,00,12,100000,50000,50000,B11,\n", 100000000+i, i)
	}

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Mid-batch upsert fails.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &EOBMF{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(sb.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eo_bmf: bulk upsert")
}

func TestEOBMF_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &EOBMF{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestEOBMF_Sync_Success(t *testing.T) {
	dir := t.TempDir()

	csvContent := eoBMFCSVHeader +
		"010202467,ALABAMA COUNCIL,,PO BOX 409,AUBURN,AL,36831,0000,03,3,1000,197103,1,15,0,1,01,202209,3,3,01,00,9,237027,22882,22882,S99,\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Mock download for all 4 regions — write CSV content to the target path.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, destPath string) {
			writeTestFixture(t, destPath, []byte(csvContent))
		}).
		Return(int64(len(csvContent)), nil).
		Times(4)

	// Expect 4 upserts (one per region).
	for range 4 {
		expectBulkUpsert(pool, "fed_data.eo_bmf", eoBMFColumns, 1)
	}

	ds := &EOBMF{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}
