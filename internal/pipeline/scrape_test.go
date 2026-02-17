package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	"github.com/sells-group/research-cli/pkg/jina"
	jinamocks "github.com/sells-group/research-cli/pkg/jina/mocks"
)

func TestScrapePhase_SearchThenScrape(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com", City: "Springfield", State: "IL"}

	// Mock Jina search returning profile URLs.
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Acme Corp BBB Profile", URL: "https://www.bbb.org/us/il/springfield/profile/construction/acme-corp-0001-12345", Content: "BBB profile"},
				{Title: "Acme Corp Google Maps", URL: "https://www.google.com/maps/place/Acme+Corp", Content: "Springfield, IL"},
				{Title: "Acme Corp - Illinois SoS", URL: "https://www.ilsos.gov/corp/acme", Content: "Acme Corp filing"},
			},
		}, nil).Maybe()

	// Mock scrape chain.
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			URL:      "https://example.com",
			Title:    "External Source",
			Markdown: "Acme Corp is a registered business entity in Springfield, IL with a BBB rating of A+.",
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pages, addrMatches := ScrapePhase(ctx, company, jinaClient, chain)

	assert.Len(t, pages, 3) // Google Maps, BBB, SoS
	for _, p := range pages {
		assert.Contains(t, p.Title, "[")
	}

	// Address cross-reference should find Springfield and IL.
	assert.NotEmpty(t, addrMatches)
}

func TestScrapePhase_ChainAllFail(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Result", URL: "https://www.bbb.org/profile/acme", Content: "content"},
				{Title: "Result", URL: "https://www.google.com/maps/place/Acme", Content: "content"},
				{Title: "Result", URL: "https://www.sos.gov/acme", Content: "Acme Corp"},
			},
		}, nil).Maybe()

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pages, _ := ScrapePhase(ctx, company, jinaClient, chain)

	assert.Len(t, pages, 0)
}

func TestScrapePhase_SearchNoResults(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Unknown Corp", URL: "https://unknown.com"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{Code: 200, Data: []jina.SearchResult{}}, nil).Maybe()

	chain := scrape.NewChain(scrape.NewPathMatcher(nil))

	pages, addrMatches := ScrapePhase(ctx, company, jinaClient, chain)

	assert.Len(t, pages, 0)
	assert.Nil(t, addrMatches)
}

func TestDefaultExternalSources(t *testing.T) {
	sources := DefaultExternalSources()

	assert.Len(t, sources, 3) // Google Maps, BBB, SoS
	assert.Equal(t, "google_maps", sources[0].Name)
	assert.Equal(t, "bbb", sources[1].Name)
	assert.Equal(t, "sos", sources[2].Name)

	// Google Maps uses direct URL; BBB and SoS use search-then-scrape.
	assert.NotNil(t, sources[0].URLFunc, "google_maps should have URLFunc")
	assert.Nil(t, sources[0].SearchQueryFunc, "google_maps should not have SearchQueryFunc")
	for _, src := range sources[1:] {
		assert.NotNil(t, src.SearchQueryFunc, "source %s should have SearchQueryFunc", src.Name)
		assert.NotNil(t, src.ResultFilter, "source %s should have ResultFilter", src.Name)
	}

	// Verify Google Maps URL includes company info and location.
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}
	mapsURL := sources[0].URLFunc(company)
	assert.Contains(t, mapsURL, "google.com/maps/search/")
	assert.Contains(t, mapsURL, "Acme")

	// Verify search queries include company info.
	for _, src := range sources[1:] {
		query, _ := src.SearchQueryFunc(company)
		assert.Contains(t, query, "Acme Corp")
	}
}
