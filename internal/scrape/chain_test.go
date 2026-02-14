package scrape

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
)

// mockScraper implements Scraper for testing.
type mockScraper struct {
	name     string
	supports bool
	result   *Result
	err      error
}

func (m *mockScraper) Name() string                                         { return m.name }
func (m *mockScraper) Supports(_ string) bool                               { return m.supports }
func (m *mockScraper) Scrape(_ context.Context, _ string) (*Result, error) { return m.result, m.err }

func TestChain_Scrape_FirstSuccess(t *testing.T) {
	matcher := NewPathMatcher([]string{"/excluded/*"})
	s1 := &mockScraper{
		name: "primary", supports: true,
		result: &Result{
			Page:   model.CrawledPage{URL: "https://acme.com", Title: "Home", Markdown: "content"},
			Source: "primary",
		},
	}
	s2 := &mockScraper{name: "fallback", supports: true}

	chain := NewChain(matcher, s1, s2)
	result, err := chain.Scrape(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, "primary", result.Source)
	assert.Equal(t, "https://acme.com", result.Page.URL)
}

func TestChain_Scrape_FallbackOnError(t *testing.T) {
	matcher := NewPathMatcher([]string{"/excluded/*"})
	s1 := &mockScraper{name: "primary", supports: true, err: errors.New("failed")}
	s2 := &mockScraper{
		name: "fallback", supports: true,
		result: &Result{
			Page:   model.CrawledPage{URL: "https://acme.com", Title: "Home"},
			Source: "fallback",
		},
	}

	chain := NewChain(matcher, s1, s2)
	result, err := chain.Scrape(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, "fallback", result.Source)
}

func TestChain_Scrape_AllFail(t *testing.T) {
	matcher := NewPathMatcher([]string{"/excluded/*"})
	s1 := &mockScraper{name: "s1", supports: true, err: errors.New("s1 error")}
	s2 := &mockScraper{name: "s2", supports: true, err: errors.New("s2 error")}

	chain := NewChain(matcher, s1, s2)
	result, err := chain.Scrape(context.Background(), "https://acme.com")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "all scrapers failed")
}

func TestChain_Scrape_ExcludedURL(t *testing.T) {
	matcher := NewPathMatcher([]string{"/blog/*"})
	s1 := &mockScraper{name: "s1", supports: true}

	chain := NewChain(matcher, s1)
	result, err := chain.Scrape(context.Background(), "https://acme.com/blog/post1")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "excluded")
}

func TestChain_Scrape_SkipsUnsupported(t *testing.T) {
	matcher := NewPathMatcher(nil)
	s1 := &mockScraper{name: "s1", supports: false}
	s2 := &mockScraper{
		name: "s2", supports: true,
		result: &Result{Page: model.CrawledPage{URL: "https://acme.com"}, Source: "s2"},
	}

	chain := NewChain(matcher, s1, s2)
	result, err := chain.Scrape(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, "s2", result.Source)
}

func TestChain_ScrapeAll(t *testing.T) {
	matcher := NewPathMatcher([]string{"/blog/*"})
	s1 := &mockScraper{
		name: "s1", supports: true,
		result: &Result{
			Page:   model.CrawledPage{URL: "fetched", Markdown: "content"},
			Source: "s1",
		},
	}

	chain := NewChain(matcher, s1)
	urls := []string{
		"https://acme.com/about",
		"https://acme.com/services",
		"https://acme.com/blog/post", // excluded
	}

	pages := chain.ScrapeAll(context.Background(), urls, 5)

	// blog/post is excluded, so only 2 pages.
	assert.Len(t, pages, 2)
}

func TestChain_ScrapeAll_Empty(t *testing.T) {
	matcher := NewPathMatcher(nil)
	s1 := &mockScraper{name: "s1", supports: true, err: errors.New("fail")}

	chain := NewChain(matcher, s1)
	pages := chain.ScrapeAll(context.Background(), []string{"https://acme.com"}, 5)

	assert.Len(t, pages, 0)
}
