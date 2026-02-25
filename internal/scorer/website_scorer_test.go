package scorer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
)

func TestWebsiteScorer_NoWebsite(t *testing.T) {
	cfg := DefaultScorerConfig()
	ws := NewWebsiteScorer(pipeline.NewLocalCrawler(), nil, cfg)

	firm := &FirmScore{
		CRDNumber: 1,
		FirmName:  "No Website LLC",
		Score:     60,
	}

	result, err := ws.Score(context.Background(), firm)
	require.NoError(t, err)
	assert.False(t, result.WebsiteReachable)
	assert.Equal(t, 55.0, result.RefinedScore, "should penalize by 5 for no website")
}

func TestWebsiteScorer_UnreachableSite(t *testing.T) {
	cfg := DefaultScorerConfig()
	ws := NewWebsiteScorer(pipeline.NewLocalCrawler(), nil, cfg)

	firm := &FirmScore{
		CRDNumber: 2,
		FirmName:  "Unreachable LLC",
		Website:   "http://192.0.2.1:1", // RFC 5737 TEST-NET, should fail to connect
		Score:     60,
	}

	result, err := ws.Score(context.Background(), firm)
	require.NoError(t, err)
	assert.False(t, result.WebsiteReachable)
	assert.Equal(t, 50.0, result.RefinedScore, "should penalize by 10 for unreachable")
}

func TestWebsiteScorer_ReachableSite(t *testing.T) {
	// Create a test server with pages.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<html><head><title>Test Firm</title></head><body>
			<h1>Welcome to Test Firm</h1>
			<p>We provide financial planning and retirement planning services.</p>
			<a href="/team">Our Team</a>
			<a href="/blog">Blog</a>
		</body></html>`)
	})
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "User-agent: *\nAllow: /")
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/team", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<html><head><title>Our Team</title></head><body>
			<h1>Our Team</h1>
			<p>John Smith, CEO and founder, is planning his retirement after 30 years.</p>
			<p>We use Tamarac and Salesforce for our operations.</p>
		</body></html>`)
	})
	mux.HandleFunc("/blog", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<html><head><title>Blog</title></head><body>
			<h1>Latest Insights</h1>
			<p>Succession planning for your practice.</p>
		</body></html>`)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	cfg := DefaultScorerConfig()
	cfg.SuccessionKeywords = []string{"retirement", "succession"}

	// The website scorer uses the crawler to probe and discover, then the chain for fetching.
	// Since we don't set up a full scrape chain, we'll test with just the crawler
	// which will discover links. The chain.ScrapeAll won't be called since chain is nil.
	ws := NewWebsiteScorer(pipeline.NewLocalCrawler(), nil, cfg)

	firm := &FirmScore{
		CRDNumber: 3,
		FirmName:  "Test Firm",
		Website:   ts.URL,
		Score:     60,
	}

	result, err := ws.Score(context.Background(), firm)
	require.NoError(t, err)
	assert.True(t, result.WebsiteReachable)
	assert.False(t, result.Blocked)
	// Without a chain, no pages are fetched, but probe body is analyzed.
	assert.GreaterOrEqual(t, result.RefinedScore, 60.0)
}

func TestWebsiteScorer_BlockedSite(t *testing.T) {
	// Simulate a Cloudflare-blocked site.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("cf-ray", "abc123")
		w.Header().Set("server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, "Checking your browser before accessing...")
	}))
	defer ts.Close()

	cfg := DefaultScorerConfig()
	ws := NewWebsiteScorer(pipeline.NewLocalCrawler(), nil, cfg)

	firm := &FirmScore{
		CRDNumber: 4,
		FirmName:  "Blocked LLC",
		Website:   ts.URL,
		Score:     70,
	}

	result, err := ws.Score(context.Background(), firm)
	require.NoError(t, err)
	assert.True(t, result.WebsiteReachable)
	assert.True(t, result.Blocked)
	assert.Equal(t, 67.0, result.RefinedScore, "should penalize by 3 for blocked")
}

func TestAnalyzeContent(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.SuccessionKeywords = []string{"retirement", "succession", "transition"}

	ws := NewWebsiteScorer(nil, nil, cfg)
	result := &WebsiteScore{
		FirmScore: FirmScore{Score: 50},
	}

	content := `We are a wealth management firm specializing in retirement planning.
Our team uses Tamarac, Salesforce, and eMoney for portfolio management.
We are exploring succession planning for our founder who is planning a transition.`

	ws.analyzeContent(result, content)

	assert.Contains(t, result.SuccessionSignals, "retirement")
	assert.Contains(t, result.SuccessionSignals, "succession")
	assert.Contains(t, result.SuccessionSignals, "transition")
	assert.Contains(t, result.TechMentions, "tamarac")
	assert.Contains(t, result.TechMentions, "salesforce")
	assert.Contains(t, result.TechMentions, "emoney")
}

func TestRefineScore(t *testing.T) {
	cfg := DefaultScorerConfig()
	ws := NewWebsiteScorer(nil, nil, cfg)

	tests := []struct {
		name    string
		base    float64
		signals WebsiteScore
		wantMin float64
		wantMax float64
	}{
		{
			name:    "no signals",
			base:    50,
			signals: WebsiteScore{},
			wantMin: 50,
			wantMax: 50,
		},
		{
			name: "team + blog",
			base: 50,
			signals: WebsiteScore{
				TeamPageFound: true,
				HasBlog:       true,
			},
			wantMin: 55,
			wantMax: 55,
		},
		{
			name: "succession signals",
			base: 50,
			signals: WebsiteScore{
				SuccessionSignals: []string{"retirement", "succession"},
			},
			wantMin: 56,
			wantMax: 56,
		},
		{
			name: "rich tech stack",
			base: 50,
			signals: WebsiteScore{
				TechMentions: []string{"tamarac", "salesforce", "emoney"},
			},
			wantMin: 55,
			wantMax: 55,
		},
		{
			name: "capped at 100",
			base: 98,
			signals: WebsiteScore{
				TeamPageFound:     true,
				HasBlog:           true,
				SuccessionSignals: []string{"retirement", "succession", "transition"},
				TechMentions:      []string{"tamarac", "salesforce", "emoney"},
			},
			wantMin: 100,
			wantMax: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.signals
			result.FirmScore = FirmScore{Score: tt.base}
			ws.refineScore(&result)
			assert.GreaterOrEqual(t, result.RefinedScore, tt.wantMin)
			assert.LessOrEqual(t, result.RefinedScore, tt.wantMax)
		})
	}
}

func TestClassifyPage(t *testing.T) {
	cfg := config.ScorerConfig{}
	ws := NewWebsiteScorer(nil, nil, cfg)

	result := &WebsiteScore{}

	ws.classifyPage(result, model.CrawledPage{URL: "https://example.com/our-team", Title: "Our Team"})
	assert.True(t, result.TeamPageFound)

	ws.classifyPage(result, model.CrawledPage{URL: "https://example.com/blog", Title: "Latest News"})
	assert.True(t, result.HasBlog)
}

func TestContainsAny(t *testing.T) {
	assert.True(t, containsAny("hello world", "hello", "foo"))
	assert.True(t, containsAny("hello world", "foo", "world"))
	assert.False(t, containsAny("hello world", "foo", "bar"))
	assert.False(t, containsAny("", "foo"))
}
