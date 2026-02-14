package pipeline

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
)

// defaultExcludePrefixes are URL path prefixes used when no config is provided.
var defaultExcludePrefixes = []string{
	"/blog/", "/blog",
	"/news/", "/news",
	"/press/", "/press",
	"/careers/", "/careers",
}

// LocalCrawler discovers links via HTTP probing and link extraction.
type LocalCrawler struct {
	http           *http.Client
	excludePaths   []string
}

// NewLocalCrawler creates a LocalCrawler with a sensible default HTTP client
// and the default exclude prefixes.
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
		excludePaths: defaultExcludePrefixes,
	}
}

// NewLocalCrawlerWithExcludes creates a LocalCrawler using the given exclude
// path patterns (glob-style, e.g. "/blog/*"). Patterns are converted to prefix
// matching by stripping the trailing "*".
func NewLocalCrawlerWithExcludes(patterns []string) *LocalCrawler {
	lc := NewLocalCrawler()
	if len(patterns) > 0 {
		lc.excludePaths = expandExcludePatterns(patterns)
	}
	return lc
}

// expandExcludePatterns converts glob patterns like "/blog/*" into prefix
// pairs like "/blog/", "/blog" for matching.
func expandExcludePatterns(patterns []string) []string {
	var prefixes []string
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Strip trailing glob star.
		p = strings.TrimRight(p, "*")
		// Ensure we match both "/blog/" and "/blog".
		p = strings.TrimRight(p, "/")
		if p != "" {
			prefixes = append(prefixes, p+"/", p)
		}
	}
	return prefixes
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
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))

	result.Reachable = true
	result.StatusCode = resp.StatusCode
	result.FinalURL = resp.Request.URL.String()

	// Block detection.
	blocked, blockType := DetectBlock(resp, body)
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
// and maxPages. Returns deduplicated URLs found.
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

	for len(queue) > 0 && len(urls) < maxPages {
		select {
		case <-ctx.Done():
			return urls, nil
		default:
		}

		item := queue[0]
		queue = queue[1:]

		urls = append(urls, item.url)

		if item.depth >= maxDepth {
			continue
		}

		links, err := lc.extractLinks(ctx, item.url, base)
		if err != nil {
			zap.L().Debug("localcrawl: error extracting links",
				zap.String("url", item.url),
				zap.Error(err),
			)
			continue
		}

		for _, link := range links {
			if seen[link] || len(urls)+len(queue) >= maxPages {
				continue
			}
			if lc.isExcluded(link, base) {
				continue
			}
			seen[link] = true
			queue = append(queue, crawlItem{url: link, depth: item.depth + 1})
		}
	}

	return urls, nil
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
	defer resp.Body.Close()

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
	resp.Body.Close()
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

func (lc *LocalCrawler) isExcluded(link string, base *url.URL) bool {
	u, err := url.Parse(link)
	if err != nil {
		return true
	}
	path := strings.ToLower(u.Path)
	for _, prefix := range lc.excludePaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// IsExcludedURL checks whether a URL matches the exclude paths. Exported for
// use by crawl.go and scrape.go to filter discovered URLs before fetching.
func (lc *LocalCrawler) IsExcludedURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return true
	}
	return lc.isExcluded(rawURL, u)
}
