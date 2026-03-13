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

func TestBLMFederalLands_Metadata(t *testing.T) {
	s := &BLMFederalLands{}
	assert.Equal(t, "blm_federal_lands", s.Name())
	assert.Equal(t, "geo.federal_lands", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestBLMFederalLands_ShouldRun(t *testing.T) {
	s := &BLMFederalLands{}
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestBLMFederalLands_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	s := &BLMFederalLands{baseURL: "http://127.0.0.1:1/blm.zip"}
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestBLMFederalLands_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	s := &BLMFederalLands{baseURL: "http://127.0.0.1:1/blm.zip"}
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFindShapefile(t *testing.T) {
	dir := t.TempDir()
	_, err := findShapefile(dir, "sma")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .shp file")
}

func TestFloatVal(t *testing.T) {
	row := []any{"42.5", 3.14, nil}
	assert.InDelta(t, 42.5, floatVal(row, 0), 0.01)
	assert.InDelta(t, 3.14, floatVal(row, 1), 0.01)
	assert.Equal(t, 0.0, floatVal(row, 2))
	assert.Equal(t, 0.0, floatVal(row, 5)) // out of range
	assert.Equal(t, 0.0, floatVal(nil, 0)) // nil row
}
