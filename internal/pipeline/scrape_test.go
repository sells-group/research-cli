package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	"github.com/sells-group/research-cli/pkg/google"
	googlemocks "github.com/sells-group/research-cli/pkg/google/mocks"
	"github.com/sells-group/research-cli/pkg/jina"
	jinamocks "github.com/sells-group/research-cli/pkg/jina/mocks"
	"github.com/sells-group/research-cli/pkg/perplexity"
)

var defaultScrapeConfig = config.ScrapeConfig{SearchTimeoutSecs: 15, SearchRetries: 0}

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

	// Mock scrape chain — return distinct content per URL so dedup doesn't collapse them.
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(
		func(_ context.Context, u string) *scrape.Result {
			return &scrape.Result{
				Page: model.CrawledPage{
					URL:      u,
					Title:    "External Source",
					Markdown: "Acme Corp in Springfield, Illinois. Source URL: " + u,
				},
				Source: "mock",
			}
		},
		func(_ context.Context, _ string) error {
			return nil
		},
	).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pages, addrMatches, sourceResults := ScrapePhase(ctx, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	assert.Len(t, pages, 3) // Google Maps, BBB, SoS (distinct content per URL)
	for _, p := range pages {
		assert.Contains(t, p.Title, "[")
	}

	// All 3 sources should have results.
	assert.Len(t, sourceResults, 3)
	for _, sr := range sourceResults {
		assert.Empty(t, sr.Error)
		assert.NotNil(t, sr.Page)
		assert.True(t, sr.Duration > 0)
	}

	// Address cross-reference should find Springfield and Illinois.
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

	pages, _, sourceResults := ScrapePhase(ctx, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	assert.Len(t, pages, 0)
	// All 3 source results should exist, each with an error.
	assert.Len(t, sourceResults, 3)
}

func TestScrapePhase_SearchNoResults(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Unknown Corp", URL: "https://unknown.com"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{Code: 200, Data: []jina.SearchResult{}}, nil).Maybe()

	chain := scrape.NewChain(scrape.NewPathMatcher(nil))

	pages, addrMatches, sourceResults := ScrapePhase(ctx, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	assert.Len(t, pages, 0)
	assert.Nil(t, addrMatches)
	assert.Len(t, sourceResults, 3) // All 3 sources attempted
}

func TestScrapePhase_PartialFailure(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	callCount := atomic.Int32{}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Acme Corp", URL: "https://www.bbb.org/us/il/profile/construction/acme-corp-0001-12345", Content: "Acme Corp BBB"},
				{Title: "Acme Corp - SoS", URL: "https://www.ilsos.gov/corp/acme", Content: "Acme Corp filing"},
			},
		}, nil).Maybe()

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(
		func(_ context.Context, u string) *scrape.Result {
			n := callCount.Add(1)
			if n == 1 {
				return &scrape.Result{
					Page: model.CrawledPage{
						URL:      u,
						Title:    "Success Page",
						Markdown: "content",
					},
					Source: "mock",
				}
			}
			return nil
		},
		func(_ context.Context, _ string) error {
			n := callCount.Load()
			if n == 1 {
				return nil
			}
			return errors.New("fail")
		},
	).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pages, _, sourceResults := ScrapePhase(ctx, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	// At least 1 page from the successful source.
	assert.GreaterOrEqual(t, len(pages), 1)
	// All 3 sources should have source results.
	assert.Len(t, sourceResults, 3)

	// At least one source result should have an error.
	hasError := false
	for _, sr := range sourceResults {
		if sr.Error != "" {
			hasError = true
		}
	}
	assert.True(t, hasError)
}

func TestScrapePhase_ContentDedup(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	identicalMarkdown := "Acme Corp, 123 Main St"

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Acme Corp BBB", URL: "https://www.bbb.org/us/il/profile/construction/acme-corp-0001-12345", Content: "Acme Corp BBB"},
				{Title: "Acme Corp SoS", URL: "https://www.ilsos.gov/corp/acme", Content: "Acme Corp filing"},
			},
		}, nil).Maybe()

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			URL:      "https://example.com",
			Title:    "Page",
			Markdown: identicalMarkdown,
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pages, _, _ := ScrapePhase(ctx, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	// All 3 sources return identical markdown → dedup to 1 page.
	assert.Equal(t, 1, len(pages))
}

func TestScrapeSource_RetryOnFailure(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp"}

	callCount := atomic.Int32{}
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(
			func(_ context.Context, _ string, _ ...jina.SearchOption) *jina.SearchResponse {
				n := callCount.Add(1)
				if n == 1 {
					return nil
				}
				return &jina.SearchResponse{
					Code: 200,
					Data: []jina.SearchResult{
						{Title: "Acme Corp BBB", URL: "https://www.bbb.org/us/il/profile/construction/acme-corp-0001-12345", Content: "Acme Corp BBB"},
					},
				}
			},
			func(_ context.Context, _ string, _ ...jina.SearchOption) error {
				if callCount.Load() == 1 {
					return errors.New("temporary failure")
				}
				return nil
			},
		).Maybe()

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			URL:      "https://example.com",
			Title:    "BBB Profile",
			Markdown: "content",
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	src := ExternalSource{
		Name: "bbb",
		SearchQueryFunc: func(c model.Company) (string, string) {
			return c.Name, "bbb.org"
		},
		ResultFilter: filterBBBResult,
	}

	cfg := config.ScrapeConfig{SearchTimeoutSecs: 5, SearchRetries: 1}
	page, err := scrapeSource(ctx, src, company, jinaClient, chain, nil, nil, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, int32(2), callCount.Load()) // Called twice: fail + retry succeed
}

func TestScrapeSource_RetryExhausted(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return((*jina.SearchResponse)(nil), errors.New("persistent failure")).Maybe()

	chain := scrape.NewChain(scrape.NewPathMatcher(nil))

	src := ExternalSource{
		Name: "bbb",
		SearchQueryFunc: func(c model.Company) (string, string) {
			return c.Name, "bbb.org"
		},
		ResultFilter: filterBBBResult,
	}

	cfg := config.ScrapeConfig{SearchTimeoutSecs: 5, SearchRetries: 1}
	page, err := scrapeSource(ctx, src, company, jinaClient, chain, nil, nil, cfg)

	assert.Error(t, err)
	assert.Nil(t, page)
	assert.Contains(t, err.Error(), "search failed")
}

func TestScrapeSource_NoDuplicatePrefix(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp"}

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			URL:      "https://www.google.com/maps/search/Acme+Corp",
			Title:    "[google_maps] Acme Corp",
			Markdown: "content",
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	src := ExternalSource{
		Name: "google_maps",
		URLFunc: func(_ model.Company) string {
			return "https://www.google.com/maps/search/Acme+Corp"
		},
	}

	page, err := scrapeSource(ctx, src, company, nil, chain, nil, nil, defaultScrapeConfig)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, "[google_maps] Acme Corp", page.Title) // Not doubled.
}

func TestContentHash(t *testing.T) {
	h1 := contentHash("hello world")
	h2 := contentHash("hello world")
	h3 := contentHash("different content")

	assert.Equal(t, h1, h2, "same input should produce same hash")
	assert.NotEqual(t, h1, h3, "different input should produce different hash")
	assert.Len(t, h1, 32, "hash should be 16 bytes = 32 hex chars")
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

func TestScrapePhase_RetryTimerCleanup(t *testing.T) {
	// Use a context that cancels quickly — well before the retry backoff (500ms)
	// would elapse. This proves that timer.Stop() in the select causes prompt
	// return on context cancellation instead of blocking for the full backoff.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	company := model.Company{Name: "Acme Corp"}

	// Jina mock that always fails, forcing the retry backoff path.
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return((*jina.SearchResponse)(nil), errors.New("always fail")).Maybe()

	chain := scrape.NewChain(scrape.NewPathMatcher(nil))

	src := ExternalSource{
		Name: "bbb",
		SearchQueryFunc: func(c model.Company) (string, string) {
			return c.Name, "bbb.org"
		},
		ResultFilter: filterBBBResult,
	}

	// Configure multiple retries with a long search timeout so the retry
	// backoff (500ms, 1s, ...) is the bottleneck, not the search timeout.
	cfg := config.ScrapeConfig{SearchTimeoutSecs: 1, SearchRetries: 3}

	start := time.Now()
	_, err := scrapeSource(ctx, src, company, jinaClient, chain, nil, nil, cfg)
	elapsed := time.Since(start)

	// scrapeSource should return ctx.Err() because context was cancelled during
	// the backoff wait between retries.
	assert.Error(t, err)

	// The key assertion: should return within 100ms (context cancelled at 30ms),
	// NOT block for the 500ms+ backoff duration. This proves timer.Stop() works.
	assert.Less(t, elapsed, 100*time.Millisecond,
		"scrapeSource should return promptly on context cancellation during retry backoff, took %v", elapsed)
}

func TestScrapePhase_EmptyName_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://acme.com"} // No Name.

	pages, addrMatches, sourceResults := ScrapePhase(ctx, company, nil, nil, nil, nil, defaultScrapeConfig)

	assert.Nil(t, pages)
	assert.Nil(t, addrMatches)
	assert.Nil(t, sourceResults)
}

func TestResolveGoogleMapsViaSearch(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string")).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{
					Title:   "Acme Corp Google Maps",
					URL:     "https://maps.google.com/place/Acme+Corp",
					Content: "Acme Corp is rated 4.5 stars (89 reviews). Open now.",
				},
			},
		}, nil)

	meta := resolveGoogleMapsViaSearch(ctx, company, jinaClient)

	assert.NotNil(t, meta)
	assert.InDelta(t, 4.5, meta.Rating, 0.001)
	assert.Equal(t, 89, meta.ReviewCount)
	assert.Equal(t, "jina_search", meta.Source)
}

func TestResolveGoogleMapsViaSearch_NoMatch(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string")).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Unrelated", URL: "https://example.com", Content: "No review data here"},
			},
		}, nil)

	meta := resolveGoogleMapsViaSearch(ctx, company, jinaClient)
	assert.Nil(t, meta)
}

func TestResolveGoogleMapsViaPerplexity(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	pplxClient := &mockPerplexityClient{
		response: "4.5 stars 89 reviews",
	}

	meta := resolveGoogleMapsViaPerplexity(ctx, company, pplxClient)

	assert.NotNil(t, meta)
	assert.InDelta(t, 4.5, meta.Rating, 0.001)
	assert.Equal(t, 89, meta.ReviewCount)
	assert.Equal(t, "perplexity", meta.Source)
}

func TestResolveGoogleMapsViaPerplexity_FreeformResponse(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	pplxClient := &mockPerplexityClient{
		response: "The rating is 4.5 with 89 reviews on Google Maps.",
	}

	meta := resolveGoogleMapsViaPerplexity(ctx, company, pplxClient)

	assert.NotNil(t, meta)
	assert.InDelta(t, 4.5, meta.Rating, 0.001)
	assert.Equal(t, 89, meta.ReviewCount)
	assert.Equal(t, "perplexity", meta.Source)
}

func TestParsePerplexityFreeform(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantRating float64
		wantCount  int
		wantNil    bool
	}{
		{
			name:       "rating is X with N reviews",
			text:       "The rating is 4.5 with 127 reviews.",
			wantRating: 4.5,
			wantCount:  127,
		},
		{
			name:       "rated X, N total reviews",
			text:       "The business is rated 4.2 on Google Maps. 89 total reviews.",
			wantRating: 4.2,
			wantCount:  89,
		},
		{
			name:       "rating of X",
			text:       "It has a rating of 3.8 and 42 reviews.",
			wantRating: 3.8,
			wantCount:  42,
		},
		{
			name:    "no rating info",
			text:    "I could not find review information for this business.",
			wantNil: true,
		},
		{
			name:    "not found",
			text:    "not found",
			wantNil: true,
		},
		{
			name:    "rating out of range",
			text:    "The rating is 6.0 with 10 reviews",
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := parsePerplexityFreeform(tt.text)
			if tt.wantNil {
				assert.Nil(t, meta)
				return
			}
			assert.NotNil(t, meta)
			assert.InDelta(t, tt.wantRating, meta.Rating, 0.001)
			assert.Equal(t, tt.wantCount, meta.ReviewCount)
		})
	}
}

func TestResolveGoogleMapsViaAPI(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	googleClient := googlemocks.NewMockClient(t)
	googleClient.On("TextSearch", mock.Anything, "Acme Corp Springfield IL").
		Return(&google.TextSearchResponse{
			Places: []google.Place{
				{
					DisplayName:     google.DisplayName{Text: "Acme Corp"},
					Rating:          4.5,
					UserRatingCount: 127,
				},
			},
		}, nil)

	meta := resolveGoogleMapsViaAPI(ctx, company, googleClient)

	assert.NotNil(t, meta)
	assert.InDelta(t, 4.5, meta.Rating, 0.001)
	assert.Equal(t, 127, meta.ReviewCount)
	assert.Equal(t, "google_api", meta.Source)
}

func TestResolveGoogleMapsViaAPI_NoResults(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Unknown Corp"}

	googleClient := googlemocks.NewMockClient(t)
	googleClient.On("TextSearch", mock.Anything, mock.AnythingOfType("string")).
		Return(&google.TextSearchResponse{}, nil)

	meta := resolveGoogleMapsViaAPI(ctx, company, googleClient)
	assert.Nil(t, meta)
}

func TestResolveGoogleMapsViaAPI_ZeroRating(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "New Corp"}

	googleClient := googlemocks.NewMockClient(t)
	googleClient.On("TextSearch", mock.Anything, mock.AnythingOfType("string")).
		Return(&google.TextSearchResponse{
			Places: []google.Place{
				{DisplayName: google.DisplayName{Text: "New Corp"}, Rating: 0, UserRatingCount: 0},
			},
		}, nil)

	meta := resolveGoogleMapsViaAPI(ctx, company, googleClient)
	assert.Nil(t, meta)
}

func TestResolveGoogleMapsFallbacks_ChainOrder(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	// Jina Search fails (no matches), Perplexity succeeds.
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string")).
		Return(&jina.SearchResponse{Code: 200, Data: []jina.SearchResult{}}, nil)

	pplxClient := &mockPerplexityClient{
		response: "4.2 stars 55 reviews",
	}

	meta := resolveGoogleMapsFallbacks(ctx, company, jinaClient, pplxClient, nil)

	assert.NotNil(t, meta)
	assert.Equal(t, "perplexity", meta.Source)
	assert.InDelta(t, 4.2, meta.Rating, 0.001)
	assert.Equal(t, 55, meta.ReviewCount)
}

func TestScrapeSource_GoogleMaps_FallbackOnScrapeFailure(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", City: "Springfield", State: "IL"}

	// Scrape chain fails completely.
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(nil, errors.New("blocked")).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	// Jina Search succeeds with review data in snippet.
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string")).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "Acme Corp", Content: "4.5 stars (89 reviews)"},
			},
		}, nil).Maybe()

	src := ExternalSource{
		Name: "google_maps",
		URLFunc: func(_ model.Company) string {
			return "https://www.google.com/maps/search/Acme+Corp"
		},
	}

	page, err := scrapeSource(ctx, src, company, jinaClient, chain, nil, nil, defaultScrapeConfig)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.NotNil(t, page.Metadata)
	assert.InDelta(t, 4.5, page.Metadata.Rating, 0.001)
	assert.Equal(t, 89, page.Metadata.ReviewCount)
	assert.Equal(t, "jina_search", page.Metadata.Source)
}

// mockPerplexityClient is a simple mock for testing Perplexity fallback.
type mockPerplexityClient struct {
	response string
	err      error
}

func (m *mockPerplexityClient) ChatCompletion(_ context.Context, _ perplexity.ChatCompletionRequest) (*perplexity.ChatCompletionResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &perplexity.ChatCompletionResponse{
		Choices: []perplexity.Choice{
			{Message: perplexity.Message{Role: "assistant", Content: m.response}},
		},
	}, nil
}
