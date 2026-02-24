package scrape

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/firecrawl"
)

// FirecrawlAdapter wraps a Firecrawl client as a Scraper for single-page scrapes.
type FirecrawlAdapter struct {
	client firecrawl.Client
}

// NewFirecrawlAdapter creates a FirecrawlAdapter from a Firecrawl client.
func NewFirecrawlAdapter(client firecrawl.Client) *FirecrawlAdapter {
	return &FirecrawlAdapter{client: client}
}

// Name implements Scraper.
func (f *FirecrawlAdapter) Name() string { return "firecrawl" }

// Supports returns true — Firecrawl can attempt any URL as a fallback.
func (f *FirecrawlAdapter) Supports(_ string) bool { return true }

// Scrape fetches a single URL via Firecrawl's scrape API.
func (f *FirecrawlAdapter) Scrape(ctx context.Context, targetURL string) (*Result, error) {
	resp, err := f.client.Scrape(ctx, firecrawl.ScrapeRequest{
		URL:     targetURL,
		Formats: []string{"markdown"},
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, eris.New("firecrawl: scrape not successful")
	}
	return &Result{
		Page: model.CrawledPage{
			URL:        resp.Data.URL,
			Title:      resp.Data.Title,
			Markdown:   resp.Data.Markdown,
			StatusCode: resp.Data.StatusCode,
		},
		Source: "firecrawl",
	}, nil
}
