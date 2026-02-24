// Package scrape provides chained web scraping for government and business registries.
package scrape

import (
	"context"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/firecrawl"
)

// Chain tries scrapers in priority order, returning the first success.
type Chain struct {
	PathMatcher *PathMatcher
	scrapers    []Scraper
	fcClient    firecrawl.Client // optional: enables batch scrape fallback
}

// NewChain creates a Chain with the given path matcher and scrapers.
// Scrapers are tried in order; the first successful result is returned.
func NewChain(matcher *PathMatcher, scrapers ...Scraper) *Chain {
	return &Chain{
		PathMatcher: matcher,
		scrapers:    scrapers,
	}
}

// WithFirecrawlClient enables batch scrape fallback for ScrapeAll.
func (c *Chain) WithFirecrawlClient(fc firecrawl.Client) *Chain {
	c.fcClient = fc
	return c
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
//
// When a Firecrawl client is set (via WithFirecrawlClient), URLs that fail
// on all non-Firecrawl scrapers are accumulated and batch-scraped via
// Firecrawl's BatchScrape API in a single call, reducing per-URL overhead.
func (c *Chain) ScrapeAll(ctx context.Context, urls []string, maxConcurrent int) []model.CrawledPage {
	var (
		mu         sync.Mutex
		pages      []model.CrawledPage
		failedURLs []string
	)

	// Try primary scrapers (all except the last one if it's Firecrawl and
	// batch mode is available).
	useBatch := c.fcClient != nil && len(c.scrapers) > 1
	primaryScrapers := c.scrapers
	if useBatch {
		// Check if last scraper is Firecrawl; if so, exclude it from primary pass.
		if c.scrapers[len(c.scrapers)-1].Name() == "firecrawl" {
			primaryScrapers = c.scrapers[:len(c.scrapers)-1]
		} else {
			useBatch = false
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrent)

	for _, u := range urls {
		g.Go(func() error {
			if c.PathMatcher.IsExcluded(u) {
				return nil
			}

			// Try primary scrapers.
			for _, s := range primaryScrapers {
				if !s.Supports(u) {
					continue
				}
				result, err := s.Scrape(gCtx, u)
				if err == nil && result != nil {
					mu.Lock()
					pages = append(pages, result.Page)
					mu.Unlock()
					return nil
				}
				if err != nil {
					zap.L().Debug("scrape: primary scraper failed",
						zap.String("scraper", s.Name()),
						zap.String("url", u),
						zap.Error(err),
					)
				}
			}

			if useBatch {
				// Collect for batch Firecrawl fallback.
				mu.Lock()
				failedURLs = append(failedURLs, u)
				mu.Unlock()
			} else {
				// Fall through to all scrapers (including Firecrawl single).
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
			}
			return nil
		})
	}

	_ = g.Wait()

	// Batch-scrape all Firecrawl fallback URLs in one API call.
	if useBatch && len(failedURLs) > 0 {
		batchPages := c.batchScrapeFirecrawl(ctx, failedURLs)
		pages = append(pages, batchPages...)
	}

	return pages
}

// batchScrapeFirecrawl sends all URLs to Firecrawl's BatchScrape API and
// polls for results.
func (c *Chain) batchScrapeFirecrawl(ctx context.Context, urls []string) []model.CrawledPage {
	zap.L().Info("scrape: batch-scraping via firecrawl",
		zap.Int("urls", len(urls)),
	)

	resp, err := c.fcClient.BatchScrape(ctx, firecrawl.BatchScrapeRequest{
		URLs:    urls,
		Formats: []string{"markdown"},
	})
	if err != nil {
		zap.L().Warn("scrape: firecrawl batch scrape failed", zap.Error(err))
		return nil
	}

	status, err := firecrawl.PollBatchScrape(ctx, c.fcClient, resp.ID,
		firecrawl.WithPollInterval(2*time.Second),
		firecrawl.WithPollCap(10*time.Second),
	)
	if err != nil {
		zap.L().Warn("scrape: firecrawl batch scrape poll failed", zap.Error(err))
		return nil
	}

	var pages []model.CrawledPage
	for _, d := range status.Data {
		if d.Markdown != "" {
			pages = append(pages, model.CrawledPage{
				URL:        d.URL,
				Title:      d.Title,
				Markdown:   d.Markdown,
				StatusCode: d.StatusCode,
			})
		}
	}

	zap.L().Info("scrape: firecrawl batch scrape complete",
		zap.Int("requested", len(urls)),
		zap.Int("received", len(pages)),
	)

	return pages
}
