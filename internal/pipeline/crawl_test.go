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
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
)

func TestCrawlPhase_CacheHit(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://acme.com", Name: "Acme"}

	st := &mockStore{}
	st.On("GetCachedCrawl", ctx, "https://acme.com").
		Return(&model.CrawlCache{
			CompanyURL: "https://acme.com",
			Pages: []model.CrawledPage{
				{URL: "https://acme.com", Title: "Home", Markdown: "Welcome"},
			},
		}, nil)

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}
	cfg := config.CrawlConfig{MaxPages: 50, MaxDepth: 2, CacheTTLHours: 24}

	result, err := CrawlPhase(ctx, company, cfg, st, jinaClient, fcClient)

	assert.NoError(t, err)
	assert.True(t, result.FromCache)
	assert.Equal(t, "cache", result.Source)
	assert.Len(t, result.Pages, 1)
	st.AssertExpectations(t)
}

func TestFetchViaJina_Success(t *testing.T) {
	ctx := context.Background()

	jinaClient := &mockJinaClient{}
	jinaClient.On("Read", ctx, "https://acme.com/about").
		Return(&jina.ReadResponse{
			Code: 200,
			Data: jina.ReadData{
				Title:   "About Acme",
				URL:     "https://acme.com/about",
				Content: "Acme Corporation is a technology company with headquarters in NYC. We build innovative solutions for enterprises worldwide.",
			},
		}, nil)

	page, err := fetchViaJina(ctx, "https://acme.com/about", jinaClient)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, "About Acme", page.Title)
	assert.Equal(t, "https://acme.com/about", page.URL)
	assert.Contains(t, page.Markdown, "Acme Corporation")
	jinaClient.AssertExpectations(t)
}

func TestFetchViaJina_NeedsFallback(t *testing.T) {
	ctx := context.Background()

	jinaClient := &mockJinaClient{}
	jinaClient.On("Read", ctx, "https://acme.com").
		Return(&jina.ReadResponse{
			Code: 200,
			Data: jina.ReadData{
				Content: "short", // Too short, triggers fallback.
			},
		}, nil)

	page, err := fetchViaJina(ctx, "https://acme.com", jinaClient)

	assert.Error(t, err)
	assert.Nil(t, page)
	jinaClient.AssertExpectations(t)
}

func TestFetchPagesWithJina_FallbackToFirecrawl(t *testing.T) {
	ctx := context.Background()

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}

	// First URL: Jina succeeds.
	jinaClient.On("Read", ctx, "https://acme.com").
		Return(&jina.ReadResponse{
			Code: 200,
			Data: jina.ReadData{
				Title:   "Acme Home",
				URL:     "https://acme.com",
				Content: "Welcome to Acme Corporation. We are a leading provider of industrial solutions with over 50 years of experience.",
			},
		}, nil)

	// Second URL: Jina fails, Firecrawl fallback succeeds.
	jinaClient.On("Read", ctx, "https://acme.com/about").
		Return(nil, assert.AnError)

	fcClient.On("Scrape", ctx, mock.AnythingOfType("firecrawl.ScrapeRequest")).
		Return(&firecrawl.ScrapeResponse{
			Success: true,
			Data: firecrawl.PageData{
				URL:      "https://acme.com/about",
				Title:    "About",
				Markdown: "About Acme Corp, a technology company.",
			},
		}, nil)

	urls := []string{"https://acme.com", "https://acme.com/about"}
	pages := fetchPagesWithJina(ctx, urls, jinaClient, fcClient)

	assert.Len(t, pages, 2)
	assert.Equal(t, "https://acme.com", pages[0].URL)
	assert.Equal(t, "https://acme.com/about", pages[1].URL)
	jinaClient.AssertExpectations(t)
	fcClient.AssertExpectations(t)
}

func TestFetchViaFirecrawlScrape_Success(t *testing.T) {
	ctx := context.Background()

	fcClient := &mockFirecrawlClient{}
	fcClient.On("Scrape", ctx, mock.AnythingOfType("firecrawl.ScrapeRequest")).
		Return(&firecrawl.ScrapeResponse{
			Success: true,
			Data: firecrawl.PageData{
				URL:        "https://acme.com",
				Title:      "Acme Home",
				Markdown:   "Welcome to Acme",
				StatusCode: 200,
			},
		}, nil)

	page, err := fetchViaFirecrawlScrape(ctx, "https://acme.com", fcClient)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, "Acme Home", page.Title)
	fcClient.AssertExpectations(t)
}

func TestFetchViaFirecrawlScrape_NotSuccessful(t *testing.T) {
	ctx := context.Background()

	fcClient := &mockFirecrawlClient{}
	fcClient.On("Scrape", ctx, mock.AnythingOfType("firecrawl.ScrapeRequest")).
		Return(&firecrawl.ScrapeResponse{
			Success: false,
		}, nil)

	page, err := fetchViaFirecrawlScrape(ctx, "https://acme.com", fcClient)

	assert.Error(t, err)
	assert.Nil(t, page)
	fcClient.AssertExpectations(t)
}

// --- CrawlPhase tests ---

func TestCrawlPhase_CacheError(t *testing.T) {
	ctx := context.Background()
	// Use an unreachable URL so probe returns Reachable=false after cache error.
	company := model.Company{URL: "http://127.0.0.1:1", Name: "Bad"}

	st := &mockStore{}
	st.On("GetCachedCrawl", ctx, "http://127.0.0.1:1").
		Return(nil, errors.New("db down"))

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}
	cfg := config.CrawlConfig{MaxPages: 50, MaxDepth: 2}

	result, err := CrawlPhase(ctx, company, cfg, st, jinaClient, fcClient)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "site unreachable")
	st.AssertExpectations(t)
}

func TestCrawlPhase_ProbeUnreachable(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "http://127.0.0.1:1", Name: "Unreachable"}

	st := &mockStore{}
	st.On("GetCachedCrawl", ctx, "http://127.0.0.1:1").
		Return(nil, nil) // Cache miss.

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}
	cfg := config.CrawlConfig{}

	result, err := CrawlPhase(ctx, company, cfg, st, jinaClient, fcClient)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "site unreachable")
	st.AssertExpectations(t)
}

// --- crawlViaFirecrawl tests ---

func TestCrawlViaFirecrawl_Success(t *testing.T) {
	ctx := context.Background()

	fcClient := &mockFirecrawlClient{}
	st := &mockStore{}
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

	fcClient := &mockFirecrawlClient{}
	st := &mockStore{}
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

	fcClient := &mockFirecrawlClient{}
	st := &mockStore{}
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

	fcClient := &mockFirecrawlClient{}
	st := &mockStore{}
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

	fcClient := &mockFirecrawlClient{}
	st := &mockStore{}
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
