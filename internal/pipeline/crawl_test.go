package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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
