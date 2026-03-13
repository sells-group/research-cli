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

func TestPADUSProtectedAreasBulk_Metadata(t *testing.T) {
	s := &PADUSProtectedAreasBulk{}
	assert.Equal(t, "padus_protected_areas_bulk", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestPADUSProtectedAreasBulk_ShouldRun(t *testing.T) {
	s := &PADUSProtectedAreasBulk{}
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestPADUSProtectedAreasBulk_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &PADUSProtectedAreasBulk{baseURL: "http://127.0.0.1:1/padus.zip"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestPADUSProtectedAreasBulk_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &PADUSProtectedAreasBulk{baseURL: "http://127.0.0.1:1/padus.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}
