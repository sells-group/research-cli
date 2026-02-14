package scrape

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathMatcher_IsExcluded(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher([]string{"/blog/*", "/news/*", "/*.pdf", "/careers/*"})

	tests := []struct {
		name     string
		url      string
		excluded bool
	}{
		{"blog post", "https://acme.com/blog/post1", true},
		{"blog root", "https://acme.com/blog", true},
		{"blog deep path", "https://acme.com/blog/2024/01/post", true},
		{"news article", "https://acme.com/news/article", true},
		{"careers job", "https://acme.com/careers/job1", true},
		{"pdf file", "https://acme.com/report.pdf", true},
		{"about page", "https://acme.com/about", false},
		{"services", "https://acme.com/services", false},
		{"homepage", "https://acme.com/", false},
		{"team", "https://acme.com/team", false},
		{"nested pdf in path", "https://acme.com/docs/report.pdf", false}, // /*.pdf only matches root-level
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.excluded, m.IsExcluded(tt.url))
		})
	}
}

func TestPathMatcher_DefaultPatterns(t *testing.T) {
	m := NewPathMatcher(nil)

	assert.True(t, m.IsExcluded("https://acme.com/blog/post"))
	assert.True(t, m.IsExcluded("https://acme.com/news/article"))
	assert.True(t, m.IsExcluded("https://acme.com/press/release"))
	assert.True(t, m.IsExcluded("https://acme.com/careers/job"))
	assert.False(t, m.IsExcluded("https://acme.com/about"))
	assert.False(t, m.IsExcluded("https://acme.com/services"))
}

func TestPathMatcher_CaseInsensitive(t *testing.T) {
	m := NewPathMatcher([]string{"/Blog/*"})

	assert.True(t, m.IsExcluded("https://acme.com/blog/post"))
	assert.True(t, m.IsExcluded("https://acme.com/BLOG/POST"))
}

func TestPathMatcher_InvalidURL(t *testing.T) {
	m := NewPathMatcher([]string{"/blog/*"})

	assert.True(t, m.IsExcluded("://invalid"))
}

func TestMatchSegmented(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
		urlPath string
		match   bool
	}{
		{"exact glob", "/blog/*", "/blog/post", true},
		{"deep path", "/blog/*", "/blog/2024/01/post", true},
		{"root match", "/blog/*", "/blog", true},
		{"no match", "/blog/*", "/about", false},
		{"pdf glob", "/*.pdf", "/report.pdf", true},
		{"nested no match", "/*.pdf", "/docs/report.pdf", false},
		{"root slash", "/blog/*", "/blog/", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.match, matchSegmented(tt.pattern, tt.urlPath))
		})
	}
}

func TestPathMatcher_Patterns(t *testing.T) {
	patterns := []string{"/blog/*", "/news/*"}
	m := NewPathMatcher(patterns)
	assert.Equal(t, patterns, m.Patterns())
}
