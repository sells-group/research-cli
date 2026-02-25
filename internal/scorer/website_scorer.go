package scorer

import (
	"context"
	"math"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/scrape"
)

// WebsiteScore extends FirmScore with website-derived intelligence.
type WebsiteScore struct {
	FirmScore
	WebsiteReachable  bool     `json:"website_reachable"`
	Blocked           bool     `json:"blocked"`
	BlockType         string   `json:"block_type,omitempty"`
	SuccessionSignals []string `json:"succession_signals,omitempty"`
	TechMentions      []string `json:"tech_mentions,omitempty"`
	TeamPageFound     bool     `json:"team_page_found"`
	HasBlog           bool     `json:"has_blog"`
	BlogFreshness     string   `json:"blog_freshness,omitempty"`
	RefinedScore      float64  `json:"refined_score"`
	PagesAnalyzed     int      `json:"pages_analyzed"`
}

// WebsiteScorer crawls a firm's website and refines the ADV-based score
// with website-derived signals.
type WebsiteScorer struct {
	crawler *pipeline.LocalCrawler
	chain   *scrape.Chain
	cfg     config.ScorerConfig
}

// NewWebsiteScorer creates a WebsiteScorer. If crawler is nil, a default
// LocalCrawler is created internally.
func NewWebsiteScorer(crawler *pipeline.LocalCrawler, chain *scrape.Chain, cfg config.ScorerConfig) *WebsiteScorer {
	if crawler == nil {
		crawler = pipeline.NewLocalCrawler()
	}
	return &WebsiteScorer{
		crawler: crawler,
		chain:   chain,
		cfg:     cfg,
	}
}

// Score crawls the firm's website and refines the ADV score.
func (ws *WebsiteScorer) Score(ctx context.Context, firm *FirmScore) (*WebsiteScore, error) {
	result := &WebsiteScore{
		FirmScore:    *firm,
		RefinedScore: firm.Score,
	}

	if firm.Website == "" {
		// No website: slight penalty, return as-is.
		result.RefinedScore = math.Max(0, firm.Score-5)
		zap.L().Debug("scorer: no website for firm",
			zap.Int("crd_number", firm.CRDNumber),
		)
		return result, nil
	}

	// Probe the website.
	probe, err := ws.crawler.Probe(ctx, firm.Website)
	if err != nil {
		return nil, eris.Wrap(err, "scorer: probe website")
	}

	result.WebsiteReachable = probe.Reachable
	if !probe.Reachable {
		result.RefinedScore = math.Max(0, firm.Score-10)
		return result, nil
	}

	if probe.Blocked {
		result.Blocked = true
		result.BlockType = probe.BlockType
		// Blocked sites get a small penalty but aren't disqualified.
		result.RefinedScore = math.Max(0, firm.Score-3)
		return result, nil
	}

	// Discover and fetch pages.
	urls, err := ws.crawler.DiscoverLinks(ctx, firm.Website, 20, 2)
	if err != nil {
		zap.L().Warn("scorer: link discovery failed",
			zap.Int("crd_number", firm.CRDNumber),
			zap.Error(err),
		)
		// Analyze just the homepage body from probe.
		if len(probe.Body) > 0 {
			ws.analyzeContent(result, string(probe.Body))
		}
		ws.refineScore(result)
		return result, nil
	}

	// Fetch pages via chain.
	var pages []model.CrawledPage
	if ws.chain != nil && len(urls) > 0 {
		pages = ws.chain.ScrapeAll(ctx, urls, 5)
	}
	result.PagesAnalyzed = len(pages)

	// Combine all page content for analysis.
	var allContent strings.Builder
	for _, p := range pages {
		ws.classifyPage(result, p)
		if p.Markdown != "" {
			allContent.WriteString(p.Markdown)
			allContent.WriteString("\n")
		}
	}

	if allContent.Len() > 0 {
		ws.analyzeContent(result, allContent.String())
	}

	ws.refineScore(result)

	zap.L().Info("scorer: website scoring complete",
		zap.Int("crd_number", firm.CRDNumber),
		zap.Float64("adv_score", firm.Score),
		zap.Float64("refined_score", result.RefinedScore),
		zap.Int("pages", result.PagesAnalyzed),
	)

	return result, nil
}

// classifyPage checks page URL/title patterns for team, blog indicators.
func (ws *WebsiteScorer) classifyPage(result *WebsiteScore, page model.CrawledPage) {
	lower := strings.ToLower(page.URL + " " + page.Title)

	if containsAny(lower, "team", "about-us", "our-team", "people", "staff", "leadership") {
		result.TeamPageFound = true
	}
	if containsAny(lower, "blog", "insights", "articles", "news", "updates") {
		result.HasBlog = true
	}
}

// analyzeContent searches page content for scoring signals.
func (ws *WebsiteScorer) analyzeContent(result *WebsiteScore, content string) {
	lower := strings.ToLower(content)

	// Succession signals.
	for _, kw := range ws.cfg.SuccessionKeywords {
		if strings.Contains(lower, strings.ToLower(kw)) {
			result.SuccessionSignals = append(result.SuccessionSignals, kw)
		}
	}

	// Tech mentions (CRM, financial planning tools).
	techKeywords := []string{
		"tamarac", "orion", "black diamond", "advyzon", "morningstar",
		"emoney", "moneyguide", "riskalyze", "redtail", "wealthbox",
		"salesforce", "hubspot",
	}
	for _, tk := range techKeywords {
		if strings.Contains(lower, tk) {
			result.TechMentions = append(result.TechMentions, tk)
		}
	}
}

// refineScore adjusts the base ADV score based on website signals.
func (ws *WebsiteScorer) refineScore(result *WebsiteScore) {
	adjustment := 0.0

	// Team page: indicates organizational depth.
	if result.TeamPageFound {
		adjustment += 3
	}

	// Blog: indicates marketing maturity.
	if result.HasBlog {
		adjustment += 2
	}

	// Succession signals from website content.
	if len(result.SuccessionSignals) > 0 {
		adjustment += math.Min(float64(len(result.SuccessionSignals))*3, 10)
	}

	// Tech stack signals operational maturity.
	if len(result.TechMentions) >= 3 {
		adjustment += 5
	} else if len(result.TechMentions) >= 1 {
		adjustment += 2
	}

	result.RefinedScore = math.Min(100, math.Max(0, result.Score+adjustment))
	result.RefinedScore = math.Round(result.RefinedScore*100) / 100
}

// containsAny checks if s contains any of the given substrings.
func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
