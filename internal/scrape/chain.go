package scrape

import (
	"context"
	"sync"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
)

// Chain tries scrapers in priority order, returning the first success.
type Chain struct {
	PathMatcher *PathMatcher
	scrapers    []Scraper
}

// NewChain creates a Chain with the given path matcher and scrapers.
// Scrapers are tried in order; the first successful result is returned.
func NewChain(matcher *PathMatcher, scrapers ...Scraper) *Chain {
	return &Chain{
		PathMatcher: matcher,
		scrapers:    scrapers,
	}
}

// Scrape tries each scraper in order for a single URL.
// Returns the first successful result, or an error if all fail.
func (c *Chain) Scrape(ctx context.Context, targetURL string) (*Result, error) {
	if c.PathMatcher.IsExcluded(targetURL) {
		return nil, eris.Errorf("scrape: url excluded by path matcher: %s", targetURL)
	}

	var lastErr error
	for _, s := range c.scrapers {
		if !s.Supports(targetURL) {
			continue
		}
		result, err := s.Scrape(ctx, targetURL)
		if err == nil && result != nil {
			return result, nil
		}
		if err != nil {
			zap.L().Debug("scrape: scraper failed, trying next",
				zap.String("scraper", s.Name()),
				zap.String("url", targetURL),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, eris.Wrap(lastErr, "scrape: all scrapers failed")
	}
	return nil, eris.Errorf("scrape: no suitable scraper for url: %s", targetURL)
}

// ScrapeAll fetches multiple URLs in parallel using the chain.
// maxConcurrent controls the concurrency limit. Failed URLs are skipped.
func (c *Chain) ScrapeAll(ctx context.Context, urls []string, maxConcurrent int) []model.CrawledPage {
	var (
		mu    sync.Mutex
		pages []model.CrawledPage
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrent)

	for _, u := range urls {
		g.Go(func() error {
			result, err := c.Scrape(gCtx, u)
			if err != nil {
				zap.L().Debug("scrape: chain failed for url",
					zap.String("url", u),
					zap.Error(err),
				)
				return nil
			}
			if result != nil {
				mu.Lock()
				pages = append(pages, result.Page)
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()
	return pages
}
