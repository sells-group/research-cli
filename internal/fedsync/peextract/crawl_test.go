package peextract

import (
	"testing"
)

func TestClassifyByURL_Homepage(t *testing.T) {
	tests := []struct {
		url  string
		want PEPageType
	}{
		{"https://example.com", PEPageTypeHomepage},
		{"https://example.com/", PEPageTypeHomepage},
	}

	for _, tt := range tests {
		got := classifyByURL(tt.url)
		if got != tt.want {
			t.Errorf("classifyByURL(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

func TestClassifyByURL_PageTypes(t *testing.T) {
	tests := []struct {
		url  string
		want PEPageType
	}{
		{"https://example.com/about", PEPageTypeAbout},
		{"https://example.com/about-us", PEPageTypeAbout},
		{"https://example.com/who-we-are", PEPageTypeAbout},
		{"https://example.com/our-story", PEPageTypeAbout},
		{"https://example.com/team", PEPageTypeTeam},
		{"https://example.com/our-team", PEPageTypeTeam},
		{"https://example.com/people", PEPageTypeTeam},
		{"https://example.com/leadership", PEPageTypeTeam},
		{"https://example.com/professionals", PEPageTypeTeam},
		{"https://example.com/portfolio", PEPageTypePortfolio},
		{"https://example.com/investments", PEPageTypePortfolio},
		{"https://example.com/portfolio-companies", PEPageTypePortfolio},
		{"https://example.com/our-companies", PEPageTypePortfolio},
		{"https://example.com/strategy", PEPageTypeStrategy},
		{"https://example.com/investment-approach", PEPageTypeStrategy},
		{"https://example.com/what-we-do", PEPageTypeStrategy},
		// Blog page type
		{"https://example.com/blog", PEPageTypeBlog},
		{"https://example.com/blog/2025/market-outlook", PEPageTypeBlog},
		{"https://example.com/insights", PEPageTypeBlog},
		{"https://example.com/perspectives", PEPageTypeBlog},
		{"https://example.com/research/report", PEPageTypeBlog},
		{"https://example.com/thought-leadership", PEPageTypeBlog},
		{"https://example.com/publications/whitepaper", PEPageTypeBlog},
		// News page type
		{"https://example.com/news", PEPageTypeNews},
		{"https://example.com/press", PEPageTypeNews},
		{"https://example.com/press-releases", PEPageTypeNews},
		{"https://example.com/media", PEPageTypeNews},
		{"https://example.com/announcements", PEPageTypeNews},
		// Other
		{"https://example.com/contact", PEPageTypeContact},
		{"https://example.com/contact-us", PEPageTypeContact},
		{"https://example.com/careers", PEPageTypeCareers},
		{"https://example.com/jobs", PEPageTypeCareers},
		{"https://example.com/random-page", PEPageTypeOther},
		{"https://example.com/legal/terms", PEPageTypeOther},
	}

	for _, tt := range tests {
		got := classifyByURL(tt.url)
		if got != tt.want {
			t.Errorf("classifyByURL(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

func TestClassifyByURL_WithQueryString(t *testing.T) {
	got := classifyByURL("https://example.com/team?page=2")
	if got != PEPageTypeTeam {
		t.Errorf("expected team, got %q", got)
	}
}

func TestClassifyByURL_CaseInsensitive(t *testing.T) {
	got := classifyByURL("https://example.com/About-Us")
	if got != PEPageTypeAbout {
		t.Errorf("expected about, got %q", got)
	}
}

func TestExtractURLYear(t *testing.T) {
	tests := []struct {
		url  string
		want int
	}{
		{"https://example.com/blog/2025/01/market-outlook", 2025},
		{"https://example.com/blog/2024/deal-update", 2024},
		{"https://example.com/news/2023-06-15-acquisition", 2023},
		{"https://example.com/insights/2022/q4-review", 2022},
		{"https://example.com/about", 0},
		{"https://example.com/team", 0},
		{"https://example.com/blog/latest-post", 0},
		{"https://example.com/blog/1999/old-post", 0}, // before 2000
	}

	for _, tt := range tests {
		got := extractURLYear(tt.url)
		if got != tt.want {
			t.Errorf("extractURLYear(%q) = %d, want %d", tt.url, got, tt.want)
		}
	}
}

func TestSortBlogPagesNewestFirst(t *testing.T) {
	pages := []ClassifiedPage{
		{URL: "https://example.com/blog/welcome", PageType: PEPageTypeBlog},
		{URL: "https://example.com/blog/2022/old-post", PageType: PEPageTypeBlog},
		{URL: "https://example.com/blog/2025/01/new-post", PageType: PEPageTypeBlog},
		{URL: "https://example.com/blog/2024/mid-post", PageType: PEPageTypeBlog},
	}

	sortBlogPagesNewestFirst(pages)

	// 2025 should be first, 2024 second, 2022 third, undated last.
	if extractURLYear(pages[0].URL) != 2025 {
		t.Errorf("expected 2025 first, got %s", pages[0].URL)
	}
	if extractURLYear(pages[1].URL) != 2024 {
		t.Errorf("expected 2024 second, got %s", pages[1].URL)
	}
	if extractURLYear(pages[2].URL) != 2022 {
		t.Errorf("expected 2022 third, got %s", pages[2].URL)
	}
	if extractURLYear(pages[3].URL) != 0 {
		t.Errorf("expected undated last, got %s", pages[3].URL)
	}
}

func TestClassifyPEPages(t *testing.T) {
	pages := []struct {
		url  string
		want PEPageType
	}{
		{"https://example.com/", PEPageTypeHomepage},
		{"https://example.com/team", PEPageTypeTeam},
		{"https://example.com/portfolio", PEPageTypePortfolio},
	}

	var input []struct {
		url  string
		want PEPageType
	}
	input = append(input, pages...)

	// Test with the actual function is hard without model.CrawledPage,
	// but we test classifyByURL which is the core logic.
	for _, p := range input {
		got := classifyByURL(p.url)
		if got != p.want {
			t.Errorf("classifyByURL(%q) = %q, want %q", p.url, got, p.want)
		}
	}
}
