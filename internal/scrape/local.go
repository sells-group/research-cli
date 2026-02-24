package scrape

import (
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// LocalScraper fetches HTML via net/http, detects blocks, and converts to
// plaintext. Free, no API calls. Falls through to Jina/Firecrawl when blocked.
type LocalScraper struct {
	client *http.Client
}

// NewLocalScraper creates a LocalScraper with sensible defaults.
func NewLocalScraper() *LocalScraper {
	return &LocalScraper{
		client: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 10 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
	}
}

func (l *LocalScraper) Name() string           { return "local_http" }
func (l *LocalScraper) Supports(_ string) bool { return true }

// Scrape fetches a URL, detects blocks, strips HTML to plaintext.
func (l *LocalScraper) Scrape(ctx context.Context, targetURL string) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "local_http: create request")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ResearchBot/1.0)")

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "local_http: fetch")
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, eris.Wrap(err, "local_http: read body")
	}

	// Block detection.
	blocked, blockType := DetectBlock(resp, body)
	if blocked {
		return nil, eris.Errorf("local_http: blocked (%s)", blockType)
	}

	if resp.StatusCode >= 400 {
		return nil, eris.Errorf("local_http: status %d", resp.StatusCode)
	}

	if len(body) < 100 {
		return nil, eris.New("local_http: empty page")
	}

	title := extractTitle(body)
	text := stripHTML(string(body))

	return &Result{
		Page: model.CrawledPage{
			URL:        targetURL,
			Title:      title,
			Markdown:   text,
			StatusCode: resp.StatusCode,
		},
		Source: "local_http",
	}, nil
}

var titleRe = regexp.MustCompile(`(?i)<title[^>]*>(.*?)</title>`)

// extractTitle pulls the <title> from HTML.
func extractTitle(body []byte) string {
	m := titleRe.FindSubmatch(body)
	if len(m) > 1 {
		return strings.TrimSpace(string(m[1]))
	}
	return ""
}

// stripHTML removes scripts/styles/nav/footer, strips tags, decodes entities,
// and collapses whitespace. The result is plaintext suitable for LLM extraction.
func stripHTML(html string) string {
	// Remove script, style, nav, footer blocks entirely.
	for _, tag := range []string{"script", "style", "nav", "footer"} {
		re := regexp.MustCompile(`(?is)<` + tag + `[^>]*>.*?</` + tag + `>`)
		html = re.ReplaceAllString(html, "")
	}

	// Strip remaining HTML tags.
	tagRe := regexp.MustCompile(`<[^>]+>`)
	html = tagRe.ReplaceAllString(html, " ")

	// Decode common HTML entities.
	r := strings.NewReplacer(
		"&amp;", "&",
		"&lt;", "<",
		"&gt;", ">",
		"&quot;", `"`,
		"&#39;", "'",
		"&nbsp;", " ",
	)
	html = r.Replace(html)

	// Collapse whitespace: multiple spaces → single, multiple newlines → double.
	spaceRe := regexp.MustCompile(`[ \t]+`)
	html = spaceRe.ReplaceAllString(html, " ")

	nlRe := regexp.MustCompile(`\n{3,}`)
	html = nlRe.ReplaceAllString(html, "\n\n")

	return strings.TrimSpace(html)
}
