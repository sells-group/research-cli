package firecrawl

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements Client for testing poll functions.
type mockClient struct {
	crawlStatusFunc       func(ctx context.Context, id string) (*CrawlStatusResponse, error)
	batchScrapeStatusFunc func(ctx context.Context, id string) (*BatchScrapeStatusResponse, error)
}

func (m *mockClient) Crawl(_ context.Context, _ CrawlRequest) (*CrawlResponse, error) {
	return nil, nil
}

func (m *mockClient) GetCrawlStatus(ctx context.Context, id string) (*CrawlStatusResponse, error) {
	return m.crawlStatusFunc(ctx, id)
}

func (m *mockClient) Scrape(_ context.Context, _ ScrapeRequest) (*ScrapeResponse, error) {
	return nil, nil
}

func (m *mockClient) BatchScrape(_ context.Context, _ BatchScrapeRequest) (*BatchScrapeResponse, error) {
	return nil, nil
}

func (m *mockClient) GetBatchScrapeStatus(ctx context.Context, id string) (*BatchScrapeStatusResponse, error) {
	return m.batchScrapeStatusFunc(ctx, id)
}

func TestPollCrawl_CompletesImmediately(t *testing.T) {
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			return &CrawlStatusResponse{
				Status: "completed",
				Total:  1,
				Data: []PageData{
					{URL: "https://example.com", Markdown: "# Home", Title: "Home", StatusCode: 200},
				},
			}, nil
		},
	}

	resp, err := PollCrawl(context.Background(), mock, "crawl-123",
		WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Len(t, resp.Data, 1)
}

func TestPollCrawl_CompletesAfterRetries(t *testing.T) {
	var calls atomic.Int32
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			n := calls.Add(1)
			if n < 3 {
				return &CrawlStatusResponse{Status: "scraping"}, nil
			}
			return &CrawlStatusResponse{
				Status: "completed",
				Total:  2,
				Data: []PageData{
					{URL: "https://example.com", Markdown: "# Home"},
					{URL: "https://example.com/about", Markdown: "# About"},
				},
			}, nil
		},
	}

	resp, err := PollCrawl(context.Background(), mock, "crawl-456",
		WithPollInterval(10*time.Millisecond),
		WithPollStep(5*time.Millisecond),
		WithPollCap(20*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Len(t, resp.Data, 2)
	assert.Equal(t, int32(3), calls.Load())
}

func TestPollCrawl_Timeout(t *testing.T) {
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			return &CrawlStatusResponse{Status: "scraping"}, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := PollCrawl(ctx, mock, "crawl-timeout",
		WithPollInterval(10*time.Millisecond),
		WithPollStep(5*time.Millisecond),
		WithPollCap(20*time.Millisecond),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollCrawl_Failed(t *testing.T) {
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			return &CrawlStatusResponse{Status: "failed"}, nil
		},
	}

	_, err := PollCrawl(context.Background(), mock, "crawl-fail",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestPollCrawl_ErrorPropagation(t *testing.T) {
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			return nil, &APIError{StatusCode: 500, Body: "server error"}
		},
	}

	_, err := PollCrawl(context.Background(), mock, "crawl-err",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 500, apiErr.StatusCode)
}

func TestPollBatchScrape_CompletesImmediately(t *testing.T) {
	mock := &mockClient{
		batchScrapeStatusFunc: func(_ context.Context, _ string) (*BatchScrapeStatusResponse, error) {
			return &BatchScrapeStatusResponse{
				Status: "completed",
				Total:  2,
				Data: []PageData{
					{URL: "https://a.com", Markdown: "# A"},
					{URL: "https://b.com", Markdown: "# B"},
				},
			}, nil
		},
	}

	resp, err := PollBatchScrape(context.Background(), mock, "batch-123",
		WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Len(t, resp.Data, 2)
}

func TestPollBatchScrape_CompletesAfterRetries(t *testing.T) {
	var calls atomic.Int32
	mock := &mockClient{
		batchScrapeStatusFunc: func(_ context.Context, _ string) (*BatchScrapeStatusResponse, error) {
			n := calls.Add(1)
			if n < 2 {
				return &BatchScrapeStatusResponse{Status: "scraping"}, nil
			}
			return &BatchScrapeStatusResponse{
				Status: "completed",
				Total:  1,
				Data:   []PageData{{URL: "https://a.com", Markdown: "# A"}},
			}, nil
		},
	}

	resp, err := PollBatchScrape(context.Background(), mock, "batch-456",
		WithPollInterval(10*time.Millisecond),
		WithPollStep(5*time.Millisecond),
		WithPollCap(20*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Equal(t, int32(2), calls.Load())
}

func TestPollBatchScrape_Timeout(t *testing.T) {
	mock := &mockClient{
		batchScrapeStatusFunc: func(_ context.Context, _ string) (*BatchScrapeStatusResponse, error) {
			return &BatchScrapeStatusResponse{Status: "scraping"}, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := PollBatchScrape(ctx, mock, "batch-timeout",
		WithPollInterval(10*time.Millisecond),
		WithPollStep(5*time.Millisecond),
		WithPollCap(20*time.Millisecond),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollBatchScrape_Failed(t *testing.T) {
	mock := &mockClient{
		batchScrapeStatusFunc: func(_ context.Context, _ string) (*BatchScrapeStatusResponse, error) {
			return &BatchScrapeStatusResponse{Status: "failed"}, nil
		},
	}

	_, err := PollBatchScrape(context.Background(), mock, "batch-fail",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestPollBatchScrape_ErrorPropagation(t *testing.T) {
	mock := &mockClient{
		batchScrapeStatusFunc: func(_ context.Context, _ string) (*BatchScrapeStatusResponse, error) {
			return nil, &APIError{StatusCode: 429, Body: "rate limited"}
		},
	}

	_, err := PollBatchScrape(context.Background(), mock, "batch-err",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 429, apiErr.StatusCode)
}

func TestPollCrawl_DefaultTimeout(t *testing.T) {
	// Verify that PollCrawl applies a default timeout when ctx has none.
	// We override the default to a short duration to avoid a long test.
	mock := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			return &CrawlStatusResponse{Status: "scraping"}, nil
		},
	}

	_, err := PollCrawl(context.Background(), mock, "crawl-default-timeout",
		WithPollInterval(5*time.Millisecond),
		WithPollStep(5*time.Millisecond),
		WithPollCap(10*time.Millisecond),
		WithPollTimeout(50*time.Millisecond),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollCrawl_ExponentialBackoffTiming(t *testing.T) {
	var timestamps []time.Time
	var mu sync.Mutex
	var calls atomic.Int32

	mc := &mockClient{
		crawlStatusFunc: func(_ context.Context, _ string) (*CrawlStatusResponse, error) {
			mu.Lock()
			timestamps = append(timestamps, time.Now())
			mu.Unlock()

			n := calls.Add(1)
			if n >= 4 {
				return &CrawlStatusResponse{Status: "completed"}, nil
			}
			return &CrawlStatusResponse{Status: "scraping"}, nil
		},
	}

	// Use short intervals for test speed
	_, err := PollCrawl(context.Background(), mc, "crawl-timing",
		WithPollInterval(50*time.Millisecond),
		WithPollCap(400*time.Millisecond),
	)
	require.NoError(t, err)
	require.Len(t, timestamps, 4)

	// Verify exponential doubling pattern:
	// Gap 0→1: ~50ms (initial)
	// Gap 1→2: ~100ms (50*2)
	// Gap 2→3: ~200ms (100*2)
	gap1 := timestamps[1].Sub(timestamps[0])
	gap2 := timestamps[2].Sub(timestamps[1])
	gap3 := timestamps[3].Sub(timestamps[2])

	// Allow generous tolerance since timing is inexact
	assert.GreaterOrEqual(t, gap1.Milliseconds(), int64(30))  // ~50ms
	assert.GreaterOrEqual(t, gap2.Milliseconds(), int64(60))  // ~100ms
	assert.GreaterOrEqual(t, gap3.Milliseconds(), int64(120)) // ~200ms

	// Verify each gap is roughly double the previous (within 3x tolerance)
	assert.Less(t, float64(gap1), float64(gap2)*1.5) // gap2 should be bigger
	assert.Less(t, float64(gap2), float64(gap3)*1.5) // gap3 should be bigger
}
