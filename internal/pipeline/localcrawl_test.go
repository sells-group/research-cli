package pipeline

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"bare domain", "acme.com", "https://acme.com/"},
		{"with https", "https://acme.com", "https://acme.com/"},
		{"with http", "http://acme.com", "http://acme.com/"},
		{"with path", "https://acme.com/about", "https://acme.com/about"},
		{"with trailing slash", "https://acme.com/", "https://acme.com/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeURL(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBaseURL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"with path", "https://acme.com/about", "https://acme.com"},
		{"with port", "http://localhost:8080/test", "http://localhost:8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := baseURL(tt.input)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestIsExcluded(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

	tests := []struct {
		name     string
		link     string
		excluded bool
	}{
		{"blog path", "https://acme.com/blog/post1", true},
		{"blog root", "https://acme.com/blog", true},
		{"news path", "https://acme.com/news/article", true},
		{"careers", "https://acme.com/careers/job1", true},
		{"press", "https://acme.com/press/release", true},
		{"about page", "https://acme.com/about", false},
		{"services", "https://acme.com/services", false},
		{"homepage", "https://acme.com/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isExcluded(tt.link, base)
			assert.Equal(t, tt.excluded, result)
		})
	}
}

func TestParseLinks(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

	html := `
	<html>
	<body>
		<a href="/about">About</a>
		<a href="/services">Services</a>
		<a href="https://acme.com/team">Team</a>
		<a href="https://external.com/page">External</a>
		<a href="#section">Anchor</a>
		<a href="javascript:void(0)">JS</a>
		<a href="mailto:info@acme.com">Email</a>
	</body>
	</html>`

	links := parseLinks(html, base)

	assert.Len(t, links, 3) // about, services, team (same host)
	assert.Contains(t, links, "https://acme.com/about")
	assert.Contains(t, links, "https://acme.com/services")
	assert.Contains(t, links, "https://acme.com/team")
}

func TestParseLinks_Deduplication(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

	html := `
	<a href="/about">About</a>
	<a href="/about">About Again</a>
	<a href="https://acme.com/about">About Full URL</a>`

	links := parseLinks(html, base)
	assert.Len(t, links, 1)
}

func TestParseLinks_RelativeResolution(t *testing.T) {
	base, _ := url.Parse("https://acme.com/pages/")

	html := `<a href="sub/page">Sub Page</a>`

	links := parseLinks(html, base)
	assert.Len(t, links, 1)
	assert.Equal(t, "https://acme.com/pages/sub/page", links[0])
}

func TestNewLocalCrawler(t *testing.T) {
	lc := NewLocalCrawler()
	assert.NotNil(t, lc)
	assert.NotNil(t, lc.http)
}
