package pipeline

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/pkg/firecrawl"
)

// CrawlPhase implements Phase 1A: discover links with LocalCrawler,
// fetch content via scrape chain (Jina primary, Firecrawl fallback).
// If probe is non-nil, it reuses the prior probe result (from Phase 0) to avoid re-probing.
func CrawlPhase(ctx context.Context, company model.Company, cfg config.CrawlConfig, st store.Store, chain *scrape.Chain, fcClient firecrawl.Client, probe *model.ProbeResult) (*model.CrawlResult, error) {
	// Check cache first.
	cached, err := st.GetCachedCrawl(ctx, company.URL)
	if err != nil {
		zap.L().Warn("crawl: cache lookup failed", zap.String("company", company.URL), zap.Error(err))
	}
	if cached != nil {
		zap.L().Info("crawl: using cached result",
			zap.String("company", company.URL),
			zap.Int("pages", len(cached.Pages)),
		)
		return &model.CrawlResult{
			Pages:      cached.Pages,
			Source:     "cache",
			FromCache:  true,
			PagesCount: len(cached.Pages),
		}, nil
	}

	lc := NewLocalCrawlerWithMatcher(chain.PathMatcher)

	// Use passed-in probe or probe fresh.
	if probe == nil {
		probe, err = lc.Probe(ctx, company.URL)
		if err != nil {
			return nil, eris.Wrap(err, "crawl: probe failed")
		}
	}

	if !probe.Reachable {
		return nil, eris.Errorf("crawl: site unreachable: %s", company.URL)
	}

	// If homepage is blocked, go straight to Firecrawl.
	if probe.Blocked {
		zap.L().Info("crawl: homepage blocked, falling back to firecrawl",
			zap.String("company", company.URL),
			zap.String("block_type", probe.BlockType),
		)
		return crawlViaFirecrawl(ctx, company.URL, cfg, fcClient, st)
	}

	// Discover links locally.
	maxPages := cfg.MaxPages
	if maxPages == 0 {
		maxPages = 50
	}
	maxDepth := cfg.MaxDepth
	if maxDepth == 0 {
		maxDepth = 2
	}

	urls, err := lc.DiscoverLinks(ctx, company.URL, maxPages, maxDepth)
	if err != nil {
		zap.L().Warn("crawl: link discovery failed, falling back to firecrawl",
			zap.String("company", company.URL),
			zap.Error(err),
		)
		return crawlViaFirecrawl(ctx, company.URL, cfg, fcClient, st)
	}

	if len(urls) == 0 {
		return crawlViaFirecrawl(ctx, company.URL, cfg, fcClient, st)
	}

	// Fetch each URL via scrape chain (Jina → Firecrawl fallback).
	pages := chain.ScrapeAll(ctx, urls, 10)

	if len(pages) == 0 {
		zap.L().Warn("crawl: no pages fetched via chain, falling back to firecrawl",
			zap.String("company", company.URL),
		)
		return crawlViaFirecrawl(ctx, company.URL, cfg, fcClient, st)
	}

	// Cache the result.
	ttl := time.Duration(cfg.CacheTTLHours) * time.Hour
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	if cacheErr := st.SetCachedCrawl(ctx, company.URL, pages, ttl); cacheErr != nil {
		zap.L().Warn("crawl: failed to cache result", zap.Error(cacheErr))
	}

	return &model.CrawlResult{
		Pages:      pages,
		Source:     "chain",
		FromCache:  false,
		PagesCount: len(pages),
	}, nil
}

func crawlViaFirecrawl(ctx context.Context, companyURL string, cfg config.CrawlConfig, client firecrawl.Client, st store.Store) (*model.CrawlResult, error) {
	maxPages := cfg.MaxPages
	if maxPages == 0 {
		maxPages = 50
	}
	maxDepth := cfg.MaxDepth
	if maxDepth == 0 {
		maxDepth = 2
	}

	crawlResp, err := client.Crawl(ctx, firecrawl.CrawlRequest{
		URL:      companyURL,
		MaxDepth: maxDepth,
		Limit:    maxPages,
	})
	if err != nil {
		return nil, eris.Wrap(err, "crawl: firecrawl start")
	}

	status, err := firecrawl.PollCrawl(ctx, client, crawlResp.ID,
		firecrawl.WithPollInterval(2*time.Second),
		firecrawl.WithPollCap(10*time.Second),
	)
	if err != nil {
		return nil, eris.Wrap(err, "crawl: firecrawl poll")
	}

	var pages []model.CrawledPage
	for _, d := range status.Data {
		pages = append(pages, model.CrawledPage{
			URL:        d.URL,
			Title:      d.Title,
			Markdown:   d.Markdown,
			StatusCode: d.StatusCode,
		})
	}

	// Cache.
	ttl := time.Duration(cfg.CacheTTLHours) * time.Hour
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	if cacheErr := st.SetCachedCrawl(ctx, companyURL, pages, ttl); cacheErr != nil {
		zap.L().Warn("crawl: failed to cache firecrawl result", zap.Error(cacheErr))
	}

	return &model.CrawlResult{
		Pages:      pages,
		Source:     "firecrawl",
		FromCache:  false,
		PagesCount: len(pages),
	}, nil
}
