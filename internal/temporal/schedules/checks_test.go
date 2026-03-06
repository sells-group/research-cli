package schedules

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
)

// mockFetcher implements fetcher.Fetcher for testing availability checks.
type mockFetcher struct {
	etag string
	err  error
}

func (m *mockFetcher) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockFetcher) DownloadToFile(_ context.Context, _ string, _ string) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (m *mockFetcher) HeadETag(_ context.Context, _ string) (string, error) {
	return m.etag, m.err
}

func (m *mockFetcher) DownloadIfChanged(_ context.Context, _ string, _ string) (io.ReadCloser, string, bool, error) {
	return nil, "", false, fmt.Errorf("not implemented")
}

// Verify mockFetcher satisfies fetcher.Fetcher at compile time.
var _ fetcher.Fetcher = (*mockFetcher)(nil)

func TestHeadCheck_NewETag(t *testing.T) {
	f := &mockFetcher{etag: "\"abc123\""}
	check := HeadCheck("https://example.com/data.zip")

	available, meta, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "\"abc123\"", meta["etag"])
	require.Equal(t, "https://example.com/data.zip", meta["url"])
}

func TestHeadCheck_SameETag(t *testing.T) {
	f := &mockFetcher{etag: "\"abc123\""}
	check := HeadCheck("https://example.com/data.zip")

	lastMeta := map[string]any{"etag": "\"abc123\""}
	available, _, err := check(context.Background(), f, nil, lastMeta)
	require.NoError(t, err)
	require.False(t, available)
}

func TestHeadCheck_DifferentETag(t *testing.T) {
	f := &mockFetcher{etag: "\"def456\""}
	check := HeadCheck("https://example.com/data.zip")

	lastMeta := map[string]any{"etag": "\"abc123\""}
	available, meta, err := check(context.Background(), f, nil, lastMeta)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "\"def456\"", meta["etag"])
}

func TestHeadCheck_Error(t *testing.T) {
	f := &mockFetcher{err: fmt.Errorf("network error")}
	check := HeadCheck("https://example.com/data.zip")

	_, _, err := check(context.Background(), f, nil, nil)
	require.Error(t, err)
}

func TestURLProbe_Success(t *testing.T) {
	f := &mockFetcher{etag: "\"xyz\""}
	check := URLProbe("https://example.com/%d/data.zip", func(_ time.Time) []any {
		return []any{2025}
	})

	available, meta, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "https://example.com/2025/data.zip", meta["url"])
	require.Equal(t, "\"xyz\"", meta["etag"])
}

func TestURLProbe_NotFound(t *testing.T) {
	f := &mockFetcher{err: fmt.Errorf("404 not found")}
	check := URLProbe("https://example.com/%d/data.zip", func(_ time.Time) []any {
		return []any{2025}
	})

	available, _, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err) // error is swallowed
	require.False(t, available)
}

func TestURLProbe_NoETag(t *testing.T) {
	f := &mockFetcher{etag: ""}
	check := URLProbe("https://example.com/%d/data.zip", func(_ time.Time) []any {
		return []any{2025}
	})

	available, meta, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "https://example.com/2025/data.zip", meta["url"])
	_, hasEtag := meta["etag"]
	require.False(t, hasEtag)
}

func TestCompositeCheck_FirstPasses(t *testing.T) {
	f := &mockFetcher{etag: "\"v1\""}
	check := CompositeCheck(
		HeadCheck("https://example.com/a.zip"),
		HeadCheck("https://example.com/b.zip"),
	)

	available, meta, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "https://example.com/a.zip", meta["url"])
}

func TestCompositeCheck_AllFail(t *testing.T) {
	f := &mockFetcher{err: fmt.Errorf("network error")}
	check := CompositeCheck(
		HeadCheck("https://example.com/a.zip"),
		HeadCheck("https://example.com/b.zip"),
	)

	available, _, err := check(context.Background(), f, nil, nil)
	require.NoError(t, err) // composite swallows errors
	require.False(t, available)
}

func TestCompositeCheck_SecondPasses(t *testing.T) {
	check := CompositeCheck(
		func(_ context.Context, _ fetcher.Fetcher, _ *time.Time, _ map[string]any) (bool, map[string]any, error) {
			return false, nil, fmt.Errorf("fail")
		},
		func(_ context.Context, _ fetcher.Fetcher, _ *time.Time, _ map[string]any) (bool, map[string]any, error) {
			return true, map[string]any{"source": "second"}, nil
		},
	)

	available, meta, err := check(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "second", meta["source"])
}
