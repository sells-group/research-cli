package pipeline

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/jina"
)

// suffixPattern matches common business entity suffixes for fuzzy name matching.
var suffixPattern = regexp.MustCompile(`(?i),?\s*(inc\.?|llc\.?|ltd\.?|co\.?|corp\.?|corporation|company|llp|lp|pllc|pc|p\.?c\.?)$`)

// sosEntityPattern matches common Secretary of State entity URL path segments.
// Uses word-boundary anchors to avoid partial matches (e.g., "/corporations" shouldn't match "/corp").
var sosEntityPattern = regexp.MustCompile(`/(entity|business|corp|filing|detail|bes|corporatellc)(/|\?|$)`)

// filterBBBResult picks the best BBB profile result from search results.
// First pass: URL on bbb.org with /profile/ and fuzzy name match in title.
// Second pass: URL on bbb.org with /profile/ and name match in URL slug.
// No blanket fallback — avoids wrong-company profiles.
func filterBBBResult(results []jina.SearchResult, company model.Company) *jina.SearchResult {
	normalized := normalizeName(company.Name)

	// First pass: title-based name match.
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if !strings.Contains(lower, "bbb.org") || !strings.Contains(lower, "/profile/") {
			continue
		}
		if fuzzyNameMatch(r.Title, company.Name) {
			return &results[i]
		}
	}

	// Second pass: slug-based name check.
	if normalized != "" {
		hyphenated := strings.ReplaceAll(normalized, " ", "-")
		for i, r := range results {
			lower := strings.ToLower(r.URL)
			if !strings.Contains(lower, "bbb.org") || !strings.Contains(lower, "/profile/") {
				continue
			}
			slug := urlSlug(r.URL)
			if slug != "" && strings.Contains(slug, hyphenated) {
				return &results[i]
			}
		}
	}

	return nil
}

// urlSlug extracts the last path segment from a URL.
func urlSlug(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	path := strings.TrimRight(parsed.Path, "/")
	if path == "" {
		return ""
	}
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return strings.ToLower(path)
	}
	return strings.ToLower(path[idx+1:])
}

// isGovOrStateURL checks if a URL belongs to a government or state business registry.
func isGovOrStateURL(lower string) bool {
	return strings.Contains(lower, ".gov") || strings.Contains(lower, ".us/") || strings.Contains(lower, ".state.")
}

// filterSoSResult picks the best Secretary of State result.
// First pass: government/state domain AND fuzzy name match.
// Second pass: government/state domain AND entity ID URL pattern (no name required).
func filterSoSResult(results []jina.SearchResult, company model.Company) *jina.SearchResult {
	// First pass: name match.
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if !isGovOrStateURL(lower) {
			continue
		}
		if fuzzyNameMatch(r.Title, company.Name) || fuzzyNameMatch(r.Content, company.Name) {
			return &results[i]
		}
	}

	// Second pass: entity ID URL pattern fallback.
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if !isGovOrStateURL(lower) {
			continue
		}
		if sosEntityPattern.MatchString(lower) {
			return &results[i]
		}
	}

	return nil
}

// fuzzyNameMatch checks if the text contains the normalized company name.
// It strips common business suffixes and does a case-insensitive substring match.
func fuzzyNameMatch(text, companyName string) bool {
	if text == "" || companyName == "" {
		return false
	}
	normalized := normalizeName(companyName)
	if normalized == "" {
		return false
	}
	return strings.Contains(strings.ToLower(text), normalized)
}

// normalizeName strips business suffixes and lowercases the name.
func normalizeName(name string) string {
	name = strings.TrimSpace(name)
	stripped := suffixPattern.ReplaceAllString(name, "")
	stripped = strings.TrimSpace(stripped)
	return strings.ToLower(stripped)
}
