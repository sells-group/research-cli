package scrape

import (
	"net/url"
	"path"
	"strings"
)

// defaultExcludePatterns are used when no custom patterns are provided.
var defaultExcludePatterns = []string{
	"/blog/*",
	"/news/*",
	"/press/*",
	"/careers/*",
}

// PathMatcher filters URLs based on glob-style path patterns.
// Uses path.Match from stdlib for proper glob matching, plus a segmented
// match so "/blog/*" matches multi-level paths like "/blog/deep/path".
type PathMatcher struct {
	patterns []string
}

// NewPathMatcher creates a PathMatcher from glob patterns (e.g. "/blog/*", "/*.pdf").
// Falls back to default patterns if none are provided.
func NewPathMatcher(patterns []string) *PathMatcher {
	if len(patterns) == 0 {
		patterns = defaultExcludePatterns
	}
	return &PathMatcher{patterns: patterns}
}

// Patterns returns the configured patterns.
func (m *PathMatcher) Patterns() []string {
	return m.patterns
}

// IsExcluded checks whether a URL matches any exclude pattern.
func (m *PathMatcher) IsExcluded(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return true
	}
	return m.isPathExcluded(u.Path)
}

// isPathExcluded checks a URL path against all patterns.
func (m *PathMatcher) isPathExcluded(urlPath string) bool {
	urlPath = strings.ToLower(urlPath)
	for _, pattern := range m.patterns {
		pattern = strings.ToLower(pattern)
		if matchSegmented(pattern, urlPath) {
			return true
		}
	}
	return false
}

// matchSegmented performs glob matching where a pattern like "/blog/*"
// matches both "/blog/post" and "/blog/deep/nested/path".
//
// It first tries an exact path.Match. If the pattern ends in "/*" and that
// fails, it also tries matching the URL path prefix against the pattern
// directory (so "/blog/*" matches "/blog/a/b/c").
func matchSegmented(pattern, urlPath string) bool {
	// Try exact stdlib glob match first.
	if ok, _ := path.Match(pattern, urlPath); ok {
		return true
	}

	// For patterns ending in "/*", check if the URL path starts with the
	// pattern's directory prefix. This lets "/blog/*" match "/blog/a/b/c".
	if strings.HasSuffix(pattern, "/*") {
		prefix := strings.TrimSuffix(pattern, "/*")
		if urlPath == prefix || strings.HasPrefix(urlPath, prefix+"/") {
			return true
		}
	}

	// For patterns ending in "/*.*" or similar, just use the exact match above.
	return false
}
