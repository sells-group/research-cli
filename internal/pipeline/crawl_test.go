package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	firecrawlmocks "github.com/sells-group/research-cli/pkg/firecrawl/mocks"
)

func testChain(t *testing.T, scrapers ...scrape.Scraper) *scrape.Chain {
	matcher := scrape.NewPathMatcher([]string{"/blog/*", "/news/*", "/press/*", "/careers/*"})
	return scrape.NewChain(matcher, scrapers...)
}

func newTestScraper(t *testing.T, name string, supports bool, result *scrape.Result, err error) *scrapemocks.MockScraper {
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return(name).Maybe()
	s.On("Supports", mock.Anything).Return(supports).Maybe()
	if supports {
		s.On("Scrape", mock.Anything, mock.Anything).Return(result, err).Maybe()
	}
	return s
}

func TestCrawlPhase_CacheHit(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://acme.com", Name: "Acme"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, "https://acme.com").
		Return(&model.CrawlCache{
			CompanyURL: "https://acme.com",
			Pages: []model.CrawledPage{
				{URL: "https://acme.com", Title: "Home", Markdown: "Welcome"},
			},
		}, nil)

	chain := testChain(t, newTestScraper(t, "s1", true, nil, nil))
	fcClient := firecrawlmocks.NewMockClient(t)
	cfg := config.CrawlConfig{MaxPages: 50, MaxDepth: 2, CacheTTLHours: 24}

	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, nil)

	assert.NoError(t, err)
	assert.True(t, result.FromCache)
	assert.Equal(t, "cache", result.Source)
	assert.Len(t, result.Pages, 1)
	st.AssertExpectations(t)
}

// --- CrawlPhase tests ---

func TestCrawlPhase_CacheError(t *testing.T) {
	ctx := context.Background()
	// Use an unreachable URL so probe returns Reachable=false after cache error.
	company := model.Company{URL: "http://127.0.0.1:1", Name: "Bad"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, "http://127.0.0.1:1").
		Return(nil, errors.New("db down"))

	chain := testChain(t, newTestScraper(t, "s1", true, nil, nil))
	fcClient := firecrawlmocks.NewMockClient(t)
	cfg := config.CrawlConfig{MaxPages: 50, MaxDepth: 2}

	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, nil)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "site unreachable")
	st.AssertExpectations(t)
}

func TestCrawlPhase_ProbeUnreachable(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "http://127.0.0.1:1", Name: "Unreachable"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, "http://127.0.0.1:1").
		Return(nil, nil) // Cache miss.

	chain := testChain(t, newTestScraper(t, "s1", true, nil, nil))
	fcClient := firecrawlmocks.NewMockClient(t)
	cfg := config.CrawlConfig{}

	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, nil)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "site unreachable")
	st.AssertExpectations(t)
}

// --- crawlViaFirecrawl tests ---

func TestCrawlViaFirecrawl_Success(t *testing.T) {
	ctx := context.Background()

	fcClient := firecrawlmocks.NewMockClient(t)
	st := storemocks.NewMockStore(t)
	cfg := config.CrawlConfig{MaxPages: 10, MaxDepth: 1, CacheTTLHours: 12}

	fcClient.On("Crawl", ctx, firecrawl.CrawlRequest{
		URL:      "https://acme.com",
		MaxDepth: 1,
		Limit:    10,
	}).Return(&firecrawl.CrawlResponse{ID: "crawl-1"}, nil)

	// PollCrawl wraps ctx with a timeout, so use mock.Anything for context.
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-1").
		Return(&firecrawl.CrawlStatusResponse{
			Status: "completed",
			Data: []firecrawl.PageData{
				{URL: "https://acme.com", Title: "Home", Markdown: "Welcome", StatusCode: 200},
				{URL: "https://acme.com/about", Title: "About", Markdown: "About us", StatusCode: 200},
			},
		}, nil)

	st.On("SetCachedCrawl", ctx, "https://acme.com", mock.AnythingOfType("[]model.CrawledPage"), 12*time.Hour).
		Return(nil)

	result, err := crawlViaFirecrawl(ctx, "https://acme.com", cfg, fcClient, st)

	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	assert.False(t, result.FromCache)
	assert.Len(t, result.Pages, 2)
	assert.Equal(t, "https://acme.com", result.Pages[0].URL)
	assert.Equal(t, "https://acme.com/about", result.Pages[1].URL)
	fcClient.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestCrawlViaFirecrawl_CrawlError(t *testing.T) {
	ctx := context.Background()

	fcClient := firecrawlmocks.NewMockClient(t)
	st := storemocks.NewMockStore(t)
	cfg := config.CrawlConfig{MaxPages: 10, MaxDepth: 1}

	fcClient.On("Crawl", ctx, mock.AnythingOfType("firecrawl.CrawlRequest")).
		Return(nil, errors.New("connection refused"))

	result, err := crawlViaFirecrawl(ctx, "https://acme.com", cfg, fcClient, st)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crawl: firecrawl start")
	fcClient.AssertExpectations(t)
}

func TestCrawlViaFirecrawl_PollError(t *testing.T) {
	ctx := context.Background()

	fcClient := firecrawlmocks.NewMockClient(t)
	st := storemocks.NewMockStore(t)
	cfg := config.CrawlConfig{MaxPages: 10, MaxDepth: 1}

	fcClient.On("Crawl", ctx, mock.AnythingOfType("firecrawl.CrawlRequest")).
		Return(&firecrawl.CrawlResponse{ID: "crawl-1"}, nil)

	// PollCrawl wraps ctx with a timeout, so use mock.Anything for context.
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-1").
		Return(nil, errors.New("api error"))

	result, err := crawlViaFirecrawl(ctx, "https://acme.com", cfg, fcClient, st)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crawl: firecrawl poll")
	fcClient.AssertExpectations(t)
}

func TestCrawlViaFirecrawl_CacheWriteError(t *testing.T) {
	ctx := context.Background()

	fcClient := firecrawlmocks.NewMockClient(t)
	st := storemocks.NewMockStore(t)
	cfg := config.CrawlConfig{MaxPages: 10, MaxDepth: 1}

	fcClient.On("Crawl", ctx, firecrawl.CrawlRequest{
		URL:      "https://acme.com",
		MaxDepth: 1,
		Limit:    10,
	}).Return(&firecrawl.CrawlResponse{ID: "crawl-1"}, nil)

	// PollCrawl wraps ctx with a timeout, so use mock.Anything for context.
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-1").
		Return(&firecrawl.CrawlStatusResponse{
			Status: "completed",
			Data: []firecrawl.PageData{
				{URL: "https://acme.com", Title: "Home", Markdown: "Welcome"},
			},
		}, nil)

	st.On("SetCachedCrawl", ctx, "https://acme.com", mock.AnythingOfType("[]model.CrawledPage"), 24*time.Hour).
		Return(errors.New("disk full"))

	result, err := crawlViaFirecrawl(ctx, "https://acme.com", cfg, fcClient, st)

	// Cache write error is non-fatal.
	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	assert.Len(t, result.Pages, 1)
	fcClient.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestCrawlViaFirecrawl_DefaultsZero(t *testing.T) {
	ctx := context.Background()

	fcClient := firecrawlmocks.NewMockClient(t)
	st := storemocks.NewMockStore(t)
	// Zero values: should default to MaxPages=50, MaxDepth=2.
	cfg := config.CrawlConfig{MaxPages: 0, MaxDepth: 0}

	fcClient.On("Crawl", ctx, firecrawl.CrawlRequest{
		URL:      "https://acme.com",
		MaxDepth: 2,
		Limit:    50,
	}).Return(&firecrawl.CrawlResponse{ID: "crawl-1"}, nil)

	// PollCrawl wraps ctx with a timeout, so use mock.Anything for context.
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-1").
		Return(&firecrawl.CrawlStatusResponse{
			Status: "completed",
			Data:   []firecrawl.PageData{},
		}, nil)

	st.On("SetCachedCrawl", ctx, "https://acme.com", mock.AnythingOfType("[]model.CrawledPage"), 24*time.Hour).
		Return(nil)

	result, err := crawlViaFirecrawl(ctx, "https://acme.com", cfg, fcClient, st)

	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	// Verify the mock was called with the expected default values (asserted by exact CrawlRequest match).
	fcClient.AssertExpectations(t)
}
