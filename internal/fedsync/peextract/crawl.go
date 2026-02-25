package peextract

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/scrape"
)

const (
	maxPEPages        = 50
	maxDepth          = 2
	scrapeConcurrency = 10
)

// PECrawlResult holds the outcome of crawling a PE firm website.
type PECrawlResult struct {
	Pages  []ClassifiedPage
	Source string // "local" or "firecrawl"
}

// ClassifiedPage is a crawled page with PE-specific classification.
type ClassifiedPage struct {
	URL        string
	Title      string
	Markdown   string
	StatusCode int
	PageType   PEPageType
}

// CrawlPEFirm crawls a PE firm website and returns classified pages.
func CrawlPEFirm(ctx context.Context, firmURL string, chain *scrape.Chain) (*PECrawlResult, error) {
	log := zap.L().With(zap.String("url", firmURL))

	// Create a local crawler with PE-specific excludes.
	lc := pipeline.NewLocalCrawlerWithExcludes([]string{
		"/careers/apply/*",
	})

	// 1. Probe the website.
	probe, err := lc.Probe(ctx, firmURL)
	if err != nil {
		return nil, eris.Wrapf(err, "peextract: probe %s", firmURL)
	}

	var pages []model.CrawledPage

	if !probe.Reachable || probe.Blocked {
		// Blocked or unreachable — use scrape chain fallback.
		log.Info("site blocked or unreachable, using scrape chain",
			zap.Bool("blocked", probe.Blocked),
			zap.String("block_type", probe.BlockType))

		if chain == nil {
			return nil, eris.Errorf("peextract: site %s is blocked and no scrape chain available", firmURL)
		}

		// Try homepage via chain (Jina/Firecrawl can often handle blocked sites).
		result, scrapeErr := chain.Scrape(ctx, firmURL)
		if scrapeErr != nil {
			log.Debug("homepage scrape failed, trying subpaths", zap.Error(scrapeErr))
		} else {
			pages = append(pages, result.Page)
		}

		// Also try common PE subpage paths (including blog/press).
		subpages := []string{
			firmURL + "/about", firmURL + "/team", firmURL + "/portfolio",
			firmURL + "/strategy", firmURL + "/contact", firmURL + "/news",
			firmURL + "/blog", firmURL + "/insights", firmURL + "/perspectives",
			firmURL + "/research", firmURL + "/press-releases", firmURL + "/media",
		}
		subResults := chain.ScrapeAll(ctx, subpages, scrapeConcurrency)
		pages = append(pages, subResults...)

		if len(pages) == 0 {
			return nil, eris.Errorf("peextract: all scrapes failed for blocked site %s", firmURL)
		}

		return &PECrawlResult{
			Pages:  classifyPEPages(pages),
			Source: "firecrawl",
		}, nil
	}

	// 2. Reachable — discover links locally.
	log.Debug("site reachable, discovering links")
	urls, err := lc.DiscoverLinks(ctx, firmURL, maxPEPages, maxDepth)
	if err != nil {
		return nil, eris.Wrapf(err, "peextract: discover links %s", firmURL)
	}

	log.Info("discovered links", zap.Int("count", len(urls)))

	if len(urls) == 0 {
		urls = []string{firmURL}
	}

	// Cap at maxPEPages.
	if len(urls) > maxPEPages {
		urls = urls[:maxPEPages]
	}

	// 3. Scrape all discovered URLs.
	if chain != nil {
		pages = chain.ScrapeAll(ctx, urls, scrapeConcurrency)
	}

	if len(pages) == 0 {
		return &PECrawlResult{Source: "local"}, nil
	}

	// 4. Classify pages.
	classified := classifyPEPages(pages)

	log.Info("crawl complete",
		zap.Int("pages", len(classified)),
		zap.String("source", "local"))

	return &PECrawlResult{
		Pages:  classified,
		Source: "local",
	}, nil
}

// classifyPEPages classifies crawled pages into PE-specific types
// and sorts blog/news pages newest-first by URL date signals.
func classifyPEPages(pages []model.CrawledPage) []ClassifiedPage {
	var result []ClassifiedPage
	for _, p := range pages {
		pt := classifyByURL(p.URL)
		result = append(result, ClassifiedPage{
			URL:        p.URL,
			Title:      sanitizeUTF8(p.Title),
			Markdown:   sanitizeUTF8(p.Markdown),
			StatusCode: p.StatusCode,
			PageType:   pt,
		})
	}

	// Sort blog and news pages newest-first so recent content gets priority.
	var blogNews []ClassifiedPage
	var other []ClassifiedPage
	for _, p := range result {
		if p.PageType == PEPageTypeBlog || p.PageType == PEPageTypeNews {
			blogNews = append(blogNews, p)
		} else {
			other = append(other, p)
		}
	}
	if len(blogNews) > 0 {
		sortBlogPagesNewestFirst(blogNews)
		result = append(other, blogNews...)
	}

	return result
}

// sanitizeUTF8 removes invalid UTF-8 byte sequences from a string.
func sanitizeUTF8(s string) string {
	// Strip null bytes — valid UTF-8 but PostgreSQL rejects them.
	s = strings.ReplaceAll(s, "\x00", "")
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "")
}

// classifyByURL uses URL path heuristics to classify a page.
func classifyByURL(rawURL string) PEPageType {
	path := strings.ToLower(rawURL)

	// Strip query string and fragment.
	if idx := strings.Index(path, "?"); idx >= 0 {
		path = path[:idx]
	}
	if idx := strings.Index(path, "#"); idx >= 0 {
		path = path[:idx]
	}

	// Strip trailing slash for matching.
	path = strings.TrimRight(path, "/")

	// Extract just the path portion.
	if idx := strings.Index(path, "://"); idx >= 0 {
		path = path[idx+3:]
		if idx2 := strings.Index(path, "/"); idx2 >= 0 {
			path = path[idx2:]
		} else {
			return PEPageTypeHomepage // just domain, no path
		}
	}

	// Root path.
	if path == "" || path == "/" {
		return PEPageTypeHomepage
	}

	// Match path patterns.
	for _, rule := range classificationRules {
		for _, pattern := range rule.patterns {
			if strings.Contains(path, pattern) {
				return rule.pageType
			}
		}
	}

	return PEPageTypeOther
}

type classificationRule struct {
	pageType PEPageType
	patterns []string
}

var classificationRules = []classificationRule{
	{PEPageTypeTeam, []string{"/team", "/people", "/leadership", "/professionals", "/our-team", "/management", "/staff", "/partners"}},
	{PEPageTypePortfolio, []string{"/portfolio", "/investments", "/companies", "/our-companies", "/portfolio-companies", "/current-investments"}},
	{PEPageTypeStrategy, []string{"/strategy", "/approach", "/investment-approach", "/thesis", "/what-we-do", "/investment-strategy", "/philosophy"}},
	{PEPageTypeAbout, []string{"/about", "/who-we-are", "/our-story", "/history", "/overview", "/firm"}},
	{PEPageTypeBlog, []string{"/blog", "/insights", "/perspectives", "/research", "/thought-leadership", "/publications"}},
	{PEPageTypeNews, []string{"/news", "/press", "/media", "/announcements", "/press-releases"}},
	{PEPageTypeContact, []string{"/contact", "/connect", "/get-in-touch", "/reach-us"}},
	{PEPageTypeCareers, []string{"/careers", "/jobs", "/opportunities", "/join", "/hiring"}},
}

// urlDateRegex matches year patterns in URLs like /blog/2025/... or /2025-01-15-post
var urlDateRegex = regexp.MustCompile(`/(\d{4})(?:/(\d{2}))?(?:/|-)`)

// extractURLYear attempts to extract a year from a URL path (e.g., /blog/2025/01/post).
// Returns 0 if no year found.
func extractURLYear(rawURL string) int {
	matches := urlDateRegex.FindStringSubmatch(rawURL)
	if len(matches) < 2 {
		return 0
	}
	year := 0
	for _, c := range matches[1] {
		year = year*10 + int(c-'0')
	}
	if year < 2000 || year > 2030 {
		return 0
	}
	return year
}

// sortBlogPagesNewestFirst sorts blog/news pages by date extracted from URLs,
// putting newest content first. Pages without date signals go last.
func sortBlogPagesNewestFirst(pages []ClassifiedPage) {
	sort.SliceStable(pages, func(i, j int) bool {
		yi := extractURLYear(pages[i].URL)
		yj := extractURLYear(pages[j].URL)
		// Pages with years sort before pages without.
		if yi != 0 && yj == 0 {
			return true
		}
		if yi == 0 && yj != 0 {
			return false
		}
		// Both have years — newer first.
		return yi > yj
	})
}
