package pipeline

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/pkg/jina"
)

// SourceResult captures the outcome of scraping a single external source.
type SourceResult struct {
	Source   string             `json:"source"`
	Page     *model.CrawledPage `json:"page,omitempty"`
	Error    string             `json:"error,omitempty"`
	Duration time.Duration      `json:"-"`
}

// ExternalSource defines an external data source to scrape.
// Sources use either search-then-scrape (SearchQueryFunc) or direct URL (URLFunc).
type ExternalSource struct {
	Name            string
	URLFunc         func(company model.Company) string
	SearchQueryFunc func(company model.Company) (query string, siteFilter string)
	ResultFilter    func(results []jina.SearchResult, company model.Company) *jina.SearchResult
	// TimeoutSecs overrides cfg.SearchTimeoutSecs for this source. 0 = use default.
	TimeoutSecs int
	// MaxRetries overrides cfg.SearchRetries for this source.
	// nil = use default from config; pointer to 0 = no retries.
	MaxRetries *int
}

// DefaultExternalSources returns the standard external sources (Google Maps, BBB, SoS).
// Google Maps uses a direct URL; BBB and SoS use search-then-scrape via Jina Search.
func DefaultExternalSources() []ExternalSource {
	return []ExternalSource{
		{
			Name: "google_maps",
			URLFunc: func(c model.Company) string {
				query := c.Name
				if c.City != "" {
					query += " " + c.City
				}
				if c.State != "" {
					query += " " + c.State
				}
				return "https://www.google.com/maps/search/" + url.QueryEscape(query)
			},
		},
		{
			Name: "bbb",
			SearchQueryFunc: func(c model.Company) (string, string) {
				query := fmt.Sprintf(`"%s"`, c.Name)
				if c.City != "" {
					query += " " + c.City
				}
				if c.State != "" {
					query += " " + c.State
				}
				return query, "bbb.org"
			},
			ResultFilter: filterBBBResult,
		},
		{
			Name: "sos",
			SearchQueryFunc: func(c model.Company) (string, string) {
				query := fmt.Sprintf(`"%s" secretary of state business entity`, c.Name)
				if c.State != "" {
					query += " " + c.State
				}
				return query, ""
			},
			ResultFilter: filterSoSResult,
			TimeoutSecs:  20,
			MaxRetries:   intPtr(1),
		},
	}
}

// ScrapePhase implements Phase 1B: fetch external sources via search-then-scrape.
// For each source, it first searches for the profile URL using Jina Search,
// then scrapes the discovered URL via the scrape chain.
// Sources are fetched in parallel. Address cross-reference metadata is returned.
func ScrapePhase(ctx context.Context, company model.Company, jinaClient jina.Client, chain *scrape.Chain, cfg config.ScrapeConfig) ([]model.CrawledPage, []AddressMatch, []SourceResult) {
	sources := DefaultExternalSources()

	var (
		mu            sync.Mutex
		sourceResults []SourceResult
	)

	g, gCtx := errgroup.WithContext(ctx)

	for _, src := range sources {
		g.Go(func() error {
			start := time.Now()
			page, err := scrapeSource(gCtx, src, company, jinaClient, chain, cfg)
			dur := time.Since(start)

			sr := SourceResult{
				Source:   src.Name,
				Page:     page,
				Duration: dur,
			}
			if err != nil {
				sr.Error = err.Error()
				zap.L().Warn("scrape: source failed",
					zap.String("source", src.Name),
					zap.Duration("duration", dur),
					zap.Error(err),
				)
			} else {
				zap.L().Debug("scrape: source complete",
					zap.String("source", src.Name),
					zap.Duration("duration", dur),
					zap.Bool("has_page", page != nil),
				)
			}

			mu.Lock()
			sourceResults = append(sourceResults, sr)
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait()

	// Extract pages from source results.
	var pages []model.CrawledPage
	for _, sr := range sourceResults {
		if sr.Page != nil {
			pages = append(pages, *sr.Page)
		}
	}

	// Dedup pages by content hash.
	pages = dedupPages(pages)

	// Cross-reference addresses from scraped pages.
	addressMatches := CrossReferenceAddress(company, pages)

	return pages, addressMatches, sourceResults
}

// contentHash returns a hex-encoded SHA-256 hash (truncated to 16 bytes) of the markdown.
func contentHash(markdown string) string {
	h := sha256.Sum256([]byte(markdown))
	return fmt.Sprintf("%x", h[:16])
}

// dedupPages removes pages with identical markdown content.
func dedupPages(pages []model.CrawledPage) []model.CrawledPage {
	seen := make(map[string]bool, len(pages))
	var result []model.CrawledPage
	for _, p := range pages {
		h := contentHash(p.Markdown)
		if seen[h] {
			zap.L().Debug("scrape: skipping duplicate page",
				zap.String("title", p.Title),
				zap.String("hash", h),
			)
			continue
		}
		seen[h] = true
		result = append(result, p)
	}
	return result
}

// scrapeSource handles the search-then-scrape flow for a single external source.
func scrapeSource(ctx context.Context, src ExternalSource, company model.Company, jinaClient jina.Client, chain *scrape.Chain, cfg config.ScrapeConfig) (*model.CrawledPage, error) {
	var targetURL string

	if src.SearchQueryFunc != nil {
		query, siteFilter := src.SearchQueryFunc(company)

		var searchOpts []jina.SearchOption
		if siteFilter != "" {
			searchOpts = append(searchOpts, jina.WithSiteFilter(siteFilter))
		}

		zap.L().Debug("scrape: searching for profile URL",
			zap.String("source", src.Name),
			zap.String("query", query),
			zap.String("site_filter", siteFilter),
		)

		timeoutSecs := cfg.SearchTimeoutSecs
		if src.TimeoutSecs > 0 {
			timeoutSecs = src.TimeoutSecs
		}
		if timeoutSecs <= 0 {
			timeoutSecs = 15
		}
		retries := cfg.SearchRetries
		if src.MaxRetries != nil {
			retries = *src.MaxRetries
		}
		maxAttempts := retries + 1
		if maxAttempts < 1 {
			maxAttempts = 1
		}

		var searchResp *jina.SearchResponse
		var searchErr error
		backoff := 500 * time.Millisecond

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			searchCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
			searchResp, searchErr = jinaClient.Search(searchCtx, query, searchOpts...)
			cancel()

			if searchErr == nil {
				break
			}

			if attempt < maxAttempts {
				zap.L().Warn("scrape: search retry",
					zap.String("source", src.Name),
					zap.Int("attempt", attempt),
					zap.Int("max_attempts", maxAttempts),
					zap.Error(searchErr),
				)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
				backoff *= 2
			}
		}
		if searchErr != nil {
			return nil, fmt.Errorf("search failed: %w", searchErr)
		}

		if len(searchResp.Data) == 0 {
			zap.L().Debug("scrape: no search results",
				zap.String("source", src.Name),
			)
			return nil, nil
		}

		// Apply result filter to pick the best hit.
		if src.ResultFilter != nil {
			best := src.ResultFilter(searchResp.Data, company)
			if best == nil {
				urls := make([]string, len(searchResp.Data))
				for i, r := range searchResp.Data {
					urls[i] = r.URL
				}
				zap.L().Debug("scrape: no result passed filter",
					zap.String("source", src.Name),
					zap.Int("candidates", len(searchResp.Data)),
					zap.String("candidate_urls", strings.Join(urls, " | ")),
				)
				return nil, nil
			}
			targetURL = best.URL
		} else {
			targetURL = searchResp.Data[0].URL
		}

		zap.L().Info("scrape: discovered profile URL",
			zap.String("source", src.Name),
			zap.String("url", targetURL),
		)
	} else if src.URLFunc != nil {
		targetURL = src.URLFunc(company)
	} else {
		return nil, nil
	}

	result, err := chain.Scrape(ctx, targetURL)
	if err != nil {
		return nil, fmt.Errorf("chain scrape failed for %s: %w", targetURL, err)
	}
	if result == nil {
		return nil, nil
	}

	page := result.Page

	// Strip boilerplate from external source pages before storing.
	page.Markdown = CleanExternalMarkdown(src.Name, page.Markdown)

	// Extract structured metadata (reviews, ratings) before title prefixing.
	page.Metadata = ParseReviewMetadata(src.Name, page.Markdown)

	// Guard against double-prefixing the title.
	prefix := fmt.Sprintf("[%s] ", src.Name)
	if !strings.HasPrefix(page.Title, prefix) {
		page.Title = prefix + page.Title
	}

	return &page, nil
}

func intPtr(v int) *int { return &v }
