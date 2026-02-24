package peextract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMergeAnswers_NoConflict(t *testing.T) {
	existing := []Answer{
		{QuestionKey: "pe_hq_address", Value: "123 Main St", Confidence: 0.9, Tier: 1},
	}
	new := []Answer{
		{QuestionKey: "pe_firm_type", Value: "private_equity", Confidence: 0.8, Tier: 1},
	}

	merged := mergeAnswers(existing, new)
	if len(merged) != 2 {
		t.Fatalf("expected 2 answers, got %d", len(merged))
	}
}

func TestMergeAnswers_HigherTierWins(t *testing.T) {
	existing := []Answer{
		{QuestionKey: "pe_investment_strategy", Value: "old", Confidence: 0.9, Tier: 1},
	}
	new := []Answer{
		{QuestionKey: "pe_investment_strategy", Value: "new synthesis", Confidence: 0.7, Tier: 2},
	}

	merged := mergeAnswers(existing, new)
	if len(merged) != 1 {
		t.Fatalf("expected 1 answer, got %d", len(merged))
	}
	if merged[0].Tier != 2 {
		t.Errorf("expected tier 2, got %d", merged[0].Tier)
	}
	if merged[0].Value != "new synthesis" {
		t.Errorf("expected 'new synthesis', got %v", merged[0].Value)
	}
}

func TestMergeAnswers_SameTierHigherConfidenceWins(t *testing.T) {
	existing := []Answer{
		{QuestionKey: "pe_year_founded", Value: 2005, Confidence: 0.5, Tier: 1},
	}
	new := []Answer{
		{QuestionKey: "pe_year_founded", Value: 2006, Confidence: 0.9, Tier: 1},
	}

	merged := mergeAnswers(existing, new)
	if len(merged) != 1 {
		t.Fatalf("expected 1 answer, got %d", len(merged))
	}
	if merged[0].Confidence != 0.9 {
		t.Errorf("expected confidence 0.9, got %f", merged[0].Confidence)
	}
}

func TestMergeAnswers_LowerConfidenceDoesNotOverride(t *testing.T) {
	existing := []Answer{
		{QuestionKey: "pe_year_founded", Value: 2005, Confidence: 0.9, Tier: 1},
	}
	new := []Answer{
		{QuestionKey: "pe_year_founded", Value: 2006, Confidence: 0.3, Tier: 1},
	}

	merged := mergeAnswers(existing, new)
	if merged[0].Confidence != 0.9 {
		t.Errorf("expected original confidence 0.9 to be kept, got %f", merged[0].Confidence)
	}
}

func TestCleanJSON(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`{"value": 42}`,
			`{"value": 42}`,
		},
		{
			"```json\n{\"value\": 42}\n```",
			`{"value": 42}`,
		},
		{
			"```\n{\"value\": 42}\n```",
			`{"value": 42}`,
		},
		{
			"Here is the answer:\n{\"value\": 42}",
			`{"value": 42}`,
		},
	}

	for _, tt := range tests {
		got := cleanJSON(tt.input)
		if got != tt.want {
			t.Errorf("cleanJSON(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRepairTruncatedJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool // whether the result should be valid JSON
	}{
		{
			name:  "already valid",
			input: `{"value": [1, 2, 3]}`,
			valid: true,
		},
		{
			name:  "unclosed array",
			input: `{"value": [{"name": "Acme"}, {"name": "Beta"}`,
			valid: true,
		},
		{
			name:  "unclosed object in array",
			input: `{"value": [{"name": "Acme"}, {"name": "Beta"`,
			valid: true,
		},
		{
			name:  "trailing comma before truncation",
			input: `{"value": [{"name": "Acme"},`,
			valid: true,
		},
		{
			name:  "deeply nested truncation",
			input: `{"value": [{"name": "Acme", "details": {"sector": "tech"`,
			valid: true,
		},
		{
			name:  "empty string",
			input: "",
			valid: false, // empty string is not valid JSON
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repaired := repairTruncatedJSON(tt.input)
			if tt.valid {
				if !json.Valid([]byte(repaired)) {
					t.Errorf("repairTruncatedJSON(%q) = %q, expected valid JSON", tt.input, repaired)
				}
			}
		})
	}
}

func TestCleanJSON_RepairsTruncation(t *testing.T) {
	// Simulates an LLM response that was truncated mid-array.
	input := `{"value": [{"name": "Fund I", "year": 2020}, {"name": "Fund II"`
	cleaned := cleanJSON(input)

	if !json.Valid([]byte(cleaned)) {
		t.Errorf("cleanJSON should repair truncated JSON, got: %q", cleaned)
	}
}

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "valid ASCII",
			input: "hello world",
			want:  "hello world",
		},
		{
			name:  "valid UTF-8",
			input: "hello \u00e9\u00e8\u00ea",
			want:  "hello \u00e9\u00e8\u00ea",
		},
		{
			name:  "invalid bytes stripped",
			input: "hello\x80\x81world",
			want:  "helloworld",
		},
		{
			name:  "mixed valid and invalid",
			input: "caf\xc3\xa9 \xfe\xff latte",
			want:  "caf\xc3\xa9  latte",
		},
		{
			name:  "null bytes stripped",
			input: "hello\x00world\x00",
			want:  "helloworld",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeUTF8(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeUTF8(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDocumentForQuestion_Routes(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID: 1,
		FirmName: "Test Partners",
		PagesByType: map[PEPageType][]ClassifiedPage{
			PEPageTypeTeam: {
				{URL: "https://example.com/team", Markdown: "Our team includes...", PageType: PEPageTypeTeam, Title: "Team"},
			},
			PEPageTypePortfolio: {
				{URL: "https://example.com/portfolio", Markdown: "Our portfolio...", PageType: PEPageTypePortfolio, Title: "Portfolio"},
			},
			PEPageTypeHomepage: {
				{URL: "https://example.com/", Markdown: "Welcome to Test Partners", PageType: PEPageTypeHomepage, Title: "Home"},
			},
		},
	}

	// Question routing to team page.
	teamQ := Question{Key: "pe_managing_partners", PageTypes: []string{"team", "about"}}
	ctx := DocumentForQuestion(docs, teamQ)
	if ctx == "" {
		t.Error("expected team page content")
	}
	if !contains(ctx, "Our team includes") {
		t.Error("expected team page markdown in context")
	}

	// Question routing to portfolio page.
	portfolioQ := Question{Key: "pe_portfolio_companies", PageTypes: []string{"portfolio"}}
	ctx = DocumentForQuestion(docs, portfolioQ)
	if !contains(ctx, "Our portfolio") {
		t.Error("expected portfolio page markdown in context")
	}

	// Question with no matching page falls back to homepage.
	strategyQ := Question{Key: "pe_investment_strategy", PageTypes: []string{"strategy"}}
	ctx = DocumentForQuestion(docs, strategyQ)
	if !contains(ctx, "Welcome to Test Partners") {
		t.Error("expected homepage fallback")
	}
}

func TestDocumentForQuestion_BlogRouting(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID: 1,
		FirmName: "Test Partners",
		PagesByType: map[PEPageType][]ClassifiedPage{
			PEPageTypeBlog: {
				{URL: "https://example.com/blog/2025/outlook", Markdown: "Blog content about market outlook...", PageType: PEPageTypeBlog, Title: "Market Outlook 2025"},
			},
			PEPageTypeNews: {
				{URL: "https://example.com/news/acquisition", Markdown: "Press release about acquisition...", PageType: PEPageTypeNews, Title: "Acquisition Announced"},
			},
			PEPageTypeHomepage: {
				{URL: "https://example.com/", Markdown: "Welcome to Test Partners", PageType: PEPageTypeHomepage, Title: "Home"},
			},
		},
	}

	// Blog question should route to blog pages.
	blogQ := Question{Key: "pe_investment_themes", Category: CatBlogIntel, PageTypes: []string{"blog", "strategy"}}
	ctx := DocumentForQuestion(docs, blogQ)
	if !contains(ctx, "Blog content about market outlook") {
		t.Error("expected blog page content for blog question")
	}

	// Blog questions with news page type should get news content.
	newsQ := Question{Key: "pe_deal_announcements", Category: CatBlogIntel, PageTypes: []string{"news", "blog", "portfolio"}}
	ctx = DocumentForQuestion(docs, newsQ)
	if !contains(ctx, "Press release about acquisition") {
		t.Error("expected news page content for deal announcements question")
	}
}

func TestDocumentForQuestion_BlogMaxDocLen(t *testing.T) {
	// Blog questions should have a larger document limit (20K vs 15K).
	// Create a page that's between the two limits.
	longContent := strings.Repeat("x", 18000)
	docs := &PEFirmDocs{
		PEFirmID: 1,
		FirmName: "Test Partners",
		PagesByType: map[PEPageType][]ClassifiedPage{
			PEPageTypeBlog: {
				{URL: "https://example.com/blog/post", Markdown: longContent, PageType: PEPageTypeBlog, Title: "Long Post"},
			},
		},
	}

	// Regular question truncates at 15K.
	regularQ := Question{Key: "pe_news_recent", Category: CatContactMisc, PageTypes: []string{"blog"}}
	regularCtx := DocumentForQuestion(docs, regularQ)

	// Blog question should include more content.
	blogQ := Question{Key: "pe_investment_themes", Category: CatBlogIntel, PageTypes: []string{"blog"}}
	blogCtx := DocumentForQuestion(docs, blogQ)

	if len(blogCtx) <= len(regularCtx) {
		t.Errorf("blog question context (%d chars) should be larger than regular (%d chars)", len(blogCtx), len(regularCtx))
	}
}

func TestDocumentForQuestion_EmptyDocs(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	q := Question{Key: "pe_hq_address", PageTypes: []string{"contact", "homepage"}}
	ctx := DocumentForQuestion(docs, q)
	if ctx != "" {
		t.Error("expected empty context for empty docs")
	}
}

func TestHasPages(t *testing.T) {
	empty := &PEFirmDocs{PagesByType: make(map[PEPageType][]ClassifiedPage)}
	if HasPages(empty) {
		t.Error("expected false for empty docs")
	}

	withPages := &PEFirmDocs{
		PagesByType: map[PEPageType][]ClassifiedPage{
			PEPageTypeHomepage: {{Markdown: "content"}},
		},
	}
	if !HasPages(withPages) {
		t.Error("expected true for docs with pages")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
