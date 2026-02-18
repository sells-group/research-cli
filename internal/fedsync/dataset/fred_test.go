package dataset

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestFRED_Metadata(t *testing.T) {
	d := &FRED{}
	assert.Equal(t, "fred", d.Name())
	assert.Equal(t, "fed_data.fred_series", d.Table())
	assert.Equal(t, Phase3, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestFRED_ShouldRun(t *testing.T) {
	d := &FRED{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced this month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 3, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced last month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}

func TestFRED_Sync_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	fredResp := fredResponse{
		Observations: []struct {
			Date  string `json:"date"`
			Value string `json:"value"`
		}{
			{Date: "2024-06-01", Value: "27610.6"},
			{Date: "2024-05-01", Value: "27400.2"},
			{Date: "2024-04-01", Value: "."}, // should be skipped
		},
	}

	// FRED downloads all 15 series in parallel. Return valid data for all.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return jsonBody(t, fredResp), nil
		}).Times(15)

	// 15 series * 2 valid obs each = 30 rows
	expectBulkUpsert(pool, "fed_data.fred_series", fredCols, 30)

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(30), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFRED_Sync_PartialFailure(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	fredResp := fredResponse{
		Observations: []struct {
			Date  string `json:"date"`
			Value string `json:"value"`
		}{
			{Date: "2024-06-01", Value: "5.33"},
		},
	}

	// One specific series returns error, the rest succeed.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "GDP")
	})).Return(nil, errors.New("network error"))

	// Remaining 14 series succeed.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return !strings.Contains(url, "GDP")
	})).RunAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return jsonBody(t, fredResp), nil
	}).Times(14)

	// 14 series * 1 obs each = 14 rows
	expectBulkUpsert(pool, "fed_data.fred_series", fredCols, 14)

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(14), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFRED_Sync_ContextCancellation(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// Allow any number of Download calls (some goroutines may start before cancellation).
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, context.Canceled).Maybe()

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "test-key"}}}
	_, err = ds.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
}

func TestFRED_Sync_EmptyResponse(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// All series return empty observations.
	emptyResp := fredResponse{Observations: nil}
	f.EXPECT().Download(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return jsonBody(t, emptyResp), nil
		}).Times(15)

	// 0 rows -> BulkUpsert returns 0, no DB expectations needed.
	// BulkUpsert with empty rows returns (0, nil) early.

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}
