package pipeline

import (
	"context"
	"encoding/xml"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
)

// LocalCrawler discovers links via HTTP probing and link extraction.
type LocalCrawler struct {
	http    *http.Client
	matcher *scrape.PathMatcher
}

// NewLocalCrawler creates a LocalCrawler with a sensible default HTTP client
// and the default exclude patterns.
func NewLocalCrawler() *LocalCrawler {
	return &LocalCrawler{
		http: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 10 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
		matcher: scrape.NewPathMatcher(nil),
	}
}

// NewLocalCrawlerWithMatcher creates a LocalCrawler using the given PathMatcher.
func NewLocalCrawlerWithMatcher(matcher *scrape.PathMatcher) *LocalCrawler {
	lc := NewLocalCrawler()
	if matcher != nil {
		lc.matcher = matcher
	}
	return lc
}

// NewLocalCrawlerWithExcludes creates a LocalCrawler using the given exclude
// path patterns (glob-style, e.g. "/blog/*"). Wraps them in a PathMatcher.
func NewLocalCrawlerWithExcludes(patterns []string) *LocalCrawler {
	return NewLocalCrawlerWithMatcher(scrape.NewPathMatcher(patterns))
}

// Probe performs an HTTP probe of the given URL checking reachability,
// robots.txt, and sitemap.xml.
func (lc *LocalCrawler) Probe(ctx context.Context, rawURL string) (*model.ProbeResult, error) {
	parsed, err := normalizeURL(rawURL)
	if err != nil {
		return nil, eris.Wrap(err, "localcrawl: parse url")
	}

	result := &model.ProbeResult{}

	// Probe homepage.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed, nil)
	if err != nil {
		return nil, eris.Wrap(err, "localcrawl: create probe request")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ResearchBot/1.0)")

	resp, err := lc.http.Do(req)
	if err != nil {
		result.Reachable = false
		return result, nil
	}
	defer resp.Body.Close() //nolint:errcheck

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))

	result.Reachable = true
	result.StatusCode = resp.StatusCode
	result.FinalURL = resp.Request.URL.String()
	result.Body = body

	// Block detection.
	blocked, blockType := scrape.DetectBlock(resp, body)
	if blocked {
		result.Blocked = true
		result.BlockType = string(blockType)
		return result, nil
	}

	// Check robots.txt and sitemap.xml in parallel.
	base := baseURL(parsed)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		result.HasRobots = lc.checkExists(ctx, base+"/robots.txt")
	}()
	go func() {
		defer wg.Done()
		result.HasSitemap = lc.checkExists(ctx, base+"/sitemap.xml")
	}()

	wg.Wait()
	return result, nil
}

// DiscoverLinks crawls a site to discover internal links up to maxDepth
// and maxPages. Merges sitemap URLs if available. Returns deduplicated URLs.
func (lc *LocalCrawler) DiscoverLinks(ctx context.Context, rawURL string, maxPages, maxDepth int) ([]string, error) {
	parsed, err := normalizeURL(rawURL)
	if err != nil {
		return nil, eris.Wrap(err, "localcrawl: parse url")
	}

	base, err := url.Parse(parsed)
	if err != nil {
		return nil, eris.Wrap(err, "localcrawl: parse base url")
	}

	seen := make(map[string]bool)
	var urls []string

	type crawlItem struct {
		url   string
		depth int
	}

	queue := []crawlItem{{url: parsed, depth: 0}}
	seen[parsed] = true

	// Seed queue from sitemap if available.
	sitemapURL := base.Scheme + "://" + base.Host + "/sitemap.xml"
	sitemapURLs := lc.fetchSitemapURLs(ctx, sitemapURL, base)
	seededCount := 0
	for _, su := range sitemapURLs {
		if seen[su] || len(queue) >= maxPages {
			continue
		}
		if lc.matcher.IsExcluded(su) {
			continue
		}
		seen[su] = true
		queue = append(queue, crawlItem{url: su, depth: 1})
		seededCount++
	}
	if seededCount > 0 {
		zap.L().Debug("localcrawl: seeded urls from sitemap",
			zap.Int("count", seededCount),
			zap.String("sitemap", sitemapURL),
		)
	}

	var mu sync.Mutex

	for {
		mu.Lock()
		if len(queue) == 0 || len(urls) >= maxPages {
			mu.Unlock()
			break
		}

		// Drain up to 5 items from the queue for parallel fetching.
		var batch []crawlItem
		for len(batch) < 5 && len(queue) > 0 && len(urls) < maxPages {
			item := queue[0]
			queue = queue[1:]
			urls = append(urls, item.url)
			if item.depth < maxDepth {
				batch = append(batch, item)
			}
		}
		mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		// Create a fresh errgroup per batch so the derived context is not
		// cancelled between iterations.
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(5)

		for _, item := range batch {
			g.Go(func() error {
				select {
				case <-gCtx.Done():
					return nil
				default:
				}

				links, err := lc.extractLinks(gCtx, item.url, base)
				if err != nil {
					zap.L().Debug("localcrawl: error extracting links",
						zap.String("url", item.url),
						zap.Error(err),
					)
					return nil
				}

				mu.Lock()
				for _, link := range links {
					if seen[link] || len(urls)+len(queue) >= maxPages {
						continue
					}
					if lc.matcher.IsExcluded(link) {
						continue
					}
					seen[link] = true
					queue = append(queue, crawlItem{url: link, depth: item.depth + 1})
				}
				mu.Unlock()
				return nil
			})
		}

		// Wait for this batch to finish before draining more from the queue.
		_ = g.Wait()
	}

	return urls, nil
}

// sitemapURLSet represents a basic sitemap.xml <urlset> document.
type sitemapURLSet struct {
	XMLName xml.Name     `xml:"urlset"`
	URLs    []sitemapLoc `xml:"url"`
}

// sitemapLoc represents a single <url><loc> entry.
type sitemapLoc struct {
	Loc string `xml:"loc"`
}

// fetchSitemapURLs fetches and parses a sitemap.xml, returning same-host URLs.
// Does NOT handle sitemap index files (<sitemapindex>).
func (lc *LocalCrawler) fetchSitemapURLs(ctx context.Context, sitemapURL string, base *url.URL) []string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sitemapURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ResearchBot/1.0)")

	resp, err := lc.http.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	// Limit to 2MB.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil
	}

	var urlSet sitemapURLSet
	if err := xml.Unmarshal(body, &urlSet); err != nil {
		return nil
	}

	var urls []string
	for _, entry := range urlSet.URLs {
		loc := strings.TrimSpace(entry.Loc)
		if loc == "" {
			continue
		}
		u, err := url.Parse(loc)
		if err != nil {
			continue
		}
		// Only same-host URLs.
		if u.Host != base.Host {
			continue
		}
		urls = append(urls, loc)
	}
	return urls
}

func (lc *LocalCrawler) extractLinks(ctx context.Context, pageURL string, base *url.URL) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "create request")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ResearchBot/1.0)")

	resp, err := lc.http.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "execute request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, eris.Wrap(err, "read body")
	}

	return parseLinks(string(body), base), nil
}

// parseLinks does a simple extraction of href attributes from HTML.
func parseLinks(html string, base *url.URL) []string {
	var links []string
	seen := make(map[string]bool)

	idx := 0
	for {
		// Find href="
		pos := strings.Index(html[idx:], "href=\"")
		if pos == -1 {
			break
		}
		idx += pos + 6

		// Find closing quote.
		end := strings.Index(html[idx:], "\"")
		if end == -1 {
			break
		}

		href := html[idx : idx+end]
		idx += end + 1

		// Skip anchors, javascript, mailto.
		if strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") {
			continue
		}

		// Resolve relative URLs.
		resolved, err := url.Parse(href)
		if err != nil {
			continue
		}
		absolute := base.ResolveReference(resolved)

		// Only keep same-host links.
		if absolute.Host != base.Host {
			continue
		}

		// Normalize: strip fragment, ensure path.
		absolute.Fragment = ""
		normalized := absolute.String()
		if !seen[normalized] {
			seen[normalized] = true
			links = append(links, normalized)
		}
	}

	return links
}

func (lc *LocalCrawler) checkExists(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ResearchBot/1.0)")

	resp, err := lc.http.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func normalizeURL(raw string) (string, error) {
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

func baseURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Scheme + "://" + u.Host
}

// IsExcludedURL checks whether a URL matches the exclude paths. Exported for
// use by crawl.go and scrape.go to filter discovered URLs before fetching.
func (lc *LocalCrawler) IsExcludedURL(rawURL string) bool {
	return lc.matcher.IsExcluded(rawURL)
}
