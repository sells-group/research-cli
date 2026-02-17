package pipeline

import (
	"regexp"
	"strings"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/jina"
)

// suffixPattern matches common business entity suffixes for fuzzy name matching.
var suffixPattern = regexp.MustCompile(`(?i),?\s*(inc\.?|llc\.?|ltd\.?|co\.?|corp\.?|corporation|company|llp|lp|pllc|pc|p\.?c\.?)$`)

// filterBBBResult picks the best BBB profile result from search results.
// It requires the URL to be on bbb.org and contain /profile/.
func filterBBBResult(results []jina.SearchResult, company model.Company) *jina.SearchResult {
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if !strings.Contains(lower, "bbb.org") {
			continue
		}
		if !strings.Contains(lower, "/profile/") {
			continue
		}
		if fuzzyNameMatch(r.Title, company.Name) {
			return &results[i]
		}
	}
	// Second pass: accept any BBB profile without name match.
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if strings.Contains(lower, "bbb.org") && strings.Contains(lower, "/profile/") {
			return &results[i]
		}
	}
	return nil
}

// isGovOrStateURL checks if a URL belongs to a government or state business registry.
func isGovOrStateURL(lower string) bool {
	return strings.Contains(lower, ".gov") || strings.Contains(lower, ".us/") || strings.Contains(lower, ".state.")
}

// filterSoSResult picks the best Secretary of State result.
// Strict: requires a government/state domain AND a fuzzy name match.
// Returns nil if no result meets both criteria — that's fine.
func filterSoSResult(results []jina.SearchResult, company model.Company) *jina.SearchResult {
	for i, r := range results {
		lower := strings.ToLower(r.URL)
		if !isGovOrStateURL(lower) {
			continue
		}
		if fuzzyNameMatch(r.Title, company.Name) || fuzzyNameMatch(r.Content, company.Name) {
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
