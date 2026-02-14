package scrape

import (
	"context"

	"github.com/sells-group/research-cli/internal/model"
)

// Result holds a scraped page with its source.
type Result struct {
	Page   model.CrawledPage
	Source string // e.g. "jina", "firecrawl"
}

// Scraper fetches a single URL and returns its content.
type Scraper interface {
	Scrape(ctx context.Context, url string) (*Result, error)
	Name() string
	Supports(url string) bool
}
