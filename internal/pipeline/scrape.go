package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/pkg/jina"
)

// ExternalSource defines an external data source to scrape.
// Sources use either search-then-scrape (SearchQueryFunc) or direct URL (URLFunc).
type ExternalSource struct {
	Name            string
	URLFunc         func(company model.Company) string
	SearchQueryFunc func(company model.Company) (query string, siteFilter string)
	ResultFilter    func(results []jina.SearchResult, company model.Company) *jina.SearchResult
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
		},
	}
}

// ScrapePhase implements Phase 1B: fetch external sources via search-then-scrape.
// For each source, it first searches for the profile URL using Jina Search,
// then scrapes the discovered URL via the scrape chain.
// Sources are fetched in parallel. Address cross-reference metadata is returned.
func ScrapePhase(ctx context.Context, company model.Company, jinaClient jina.Client, chain *scrape.Chain) ([]model.CrawledPage, []AddressMatch) {
	sources := DefaultExternalSources()

	var (
		mu    sync.Mutex
		pages []model.CrawledPage
	)

	g, gCtx := errgroup.WithContext(ctx)

	for _, src := range sources {
		g.Go(func() error {
			page, err := scrapeSource(gCtx, src, company, jinaClient, chain)
			if err != nil {
				zap.L().Warn("scrape: source failed",
					zap.String("source", src.Name),
					zap.Error(err),
				)
				return nil
			}
			if page != nil {
				mu.Lock()
				pages = append(pages, *page)
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()

	// Cross-reference addresses from scraped pages.
	addressMatches := CrossReferenceAddress(company, pages)

	return pages, addressMatches
}

// scrapeSource handles the search-then-scrape flow for a single external source.
func scrapeSource(ctx context.Context, src ExternalSource, company model.Company, jinaClient jina.Client, chain *scrape.Chain) (*model.CrawledPage, error) {
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

		searchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		searchResp, err := jinaClient.Search(searchCtx, query, searchOpts...)
		if err != nil {
			return nil, fmt.Errorf("search failed: %w", err)
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
	page.Title = fmt.Sprintf("[%s] %s", src.Name, page.Title)
	return &page, nil
}
