package dataset

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestCBP_Metadata(t *testing.T) {
	ds := &CBP{}
	assert.Equal(t, "cbp", ds.Name())
	assert.Equal(t, "fed_data.cbp_data", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestCBP_ShouldRun(t *testing.T) {
	ds := &CBP{}

	// Never synced -> should run
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year -> should run (past March release)
	lastYear := time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after release -> should not run
	thisYear := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date -> should not run (even if synced long ago)
	beforeRelease := time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(beforeRelease, &lastYear))

	// Exactly on March 1 -> should run if last sync was before
	marchFirst := time.Date(2024, time.March, 2, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(marchFirst, &lastYear))

	// Synced January of this year, now is April -> should run (synced before March release)
	janSync := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &janSync))
}

func TestCBP_Sync_EmptyCSV(t *testing.T) {
	dir := t.TempDir()

	// CSV with only header, no data rows. All years get this empty CSV.
	csvContent := "fipstate,fipscty,naics,emp,emp_nf,qp1,qp1_nf,ap,ap_nf,est\n"

	zipPath := createTestZip(t, dir, "cbp_empty.zip", "cbp19co.csv", csvContent)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	numYears := currentDataYear() - cbpStartYear + 1
	mockDownloadToFile(f, zipPath).Times(numYears * 2) // county + state per year

	// No BulkUpsert expected since 0 rows pass.

	ds := &CBP{}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCBP_Sync_YearFailure(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// All years fail to download.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &CBP{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
}
