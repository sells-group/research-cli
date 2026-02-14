package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
)

func TestScrapePhase_ChainSuccess(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	chain := scrape.NewChain(
		scrape.NewPathMatcher(nil),
		&mockScraper{
			name: "mock", supports: true,
			result: &scrape.Result{
				Page: model.CrawledPage{
					URL:      "https://example.com",
					Title:    "External Source",
					Markdown: "Acme Corp is a registered business entity with a BBB rating of A+.",
				},
				Source: "mock",
			},
		},
	)

	pages := ScrapePhase(ctx, company, chain)

	assert.Len(t, pages, 3) // GP, BBB, SoS (PPP handled by Phase 1D)
	for _, p := range pages {
		assert.Contains(t, p.Title, "[")
	}
}

func TestScrapePhase_ChainAllFail(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	chain := scrape.NewChain(
		scrape.NewPathMatcher(nil),
		&mockScraper{name: "mock", supports: true, err: errors.New("fail")},
	)

	pages := ScrapePhase(ctx, company, chain)

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
