package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestBLMMineralLeases_Metadata(t *testing.T) {
	s := &BLMMineralLeases{}
	assert.Equal(t, "blm_mineral_leases", s.Name())
	assert.Equal(t, "geo.mineral_leases", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestBLMMineralLeases_ShouldRun(t *testing.T) {
	s := &BLMMineralLeases{}
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestBLMMineralLeases_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &BLMMineralLeases{baseURL: "http://127.0.0.1:1/blm_mineral.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestBLMMineralLeases_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &BLMMineralLeases{baseURL: "http://127.0.0.1:1/blm_mineral.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}
