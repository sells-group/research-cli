package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
)

func TestScrapePhase_JinaSuccess(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}

	// Jina succeeds for all sources (use mock.Anything for ctx since errgroup wraps it).
	jinaClient.On("Read", mock.Anything, mock.AnythingOfType("string")).
		Return(&jina.ReadResponse{
			Code: 200,
			Data: jina.ReadData{
				Title:   "External Source",
				URL:     "https://example.com",
				Content: "Acme Corp is a registered business entity with a BBB rating of A+. They have been in operation since 2010 and provide technology services.",
			},
		}, nil)

	pages := ScrapePhase(ctx, company, jinaClient, fcClient)

	assert.Len(t, pages, 3) // GP, BBB, SoS (PPP handled by Phase 1D)
	for _, p := range pages {
		assert.Contains(t, p.Title, "[")
	}
	jinaClient.AssertExpectations(t)
}

func TestScrapePhase_JinaFails_FirecrawlFallback(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}

	// Jina fails for all requests (use mock.Anything for ctx).
	jinaClient.On("Read", mock.Anything, mock.AnythingOfType("string")).
		Return(nil, assert.AnError)

	// Firecrawl succeeds.
	fcClient.On("Scrape", mock.Anything, mock.AnythingOfType("firecrawl.ScrapeRequest")).
		Return(&firecrawl.ScrapeResponse{
			Success: true,
			Data: firecrawl.PageData{
				URL:      "https://example.com",
				Title:    "External",
				Markdown: "Some external content about the company.",
			},
		}, nil)

	pages := ScrapePhase(ctx, company, jinaClient, fcClient)

	assert.Len(t, pages, 3)
	jinaClient.AssertExpectations(t)
	fcClient.AssertExpectations(t)
}

func TestScrapePhase_BothFail(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	jinaClient := &mockJinaClient{}
	fcClient := &mockFirecrawlClient{}

	jinaClient.On("Read", mock.Anything, mock.AnythingOfType("string")).
		Return(nil, assert.AnError)

	fcClient.On("Scrape", mock.Anything, mock.AnythingOfType("firecrawl.ScrapeRequest")).
		Return(nil, assert.AnError)

	pages := ScrapePhase(ctx, company, jinaClient, fcClient)

	assert.Len(t, pages, 0)
}

func TestDefaultExternalSources(t *testing.T) {
	company := model.Company{Name: "Acme Corp"}
	sources := DefaultExternalSources(company)

	assert.Len(t, sources, 3) // GP, BBB, SoS (PPP handled by Phase 1D)
	assert.Equal(t, "google_places", sources[0].Name)
	assert.Equal(t, "bbb", sources[1].Name)
	assert.Equal(t, "sos", sources[2].Name)

	// Verify each source generates a URL.
	for _, src := range sources {
		url := src.URLFunc(company)
		assert.NotEmpty(t, url)
		assert.Contains(t, url, "Acme Corp")
	}
}
