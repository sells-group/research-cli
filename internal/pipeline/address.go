// Package pipeline orchestrates the multi-phase company enrichment workflow.
package pipeline

import (
	"strings"

	"github.com/sells-group/research-cli/internal/model"
)

// AddressMatch holds the result of cross-referencing a company's address
// against content found on a scraped page.
type AddressMatch struct {
	Source     string  `json:"source"`
	Address    string  `json:"address"`
	CityMatch  bool    `json:"city_match"`
	StateMatch bool    `json:"state_match"`
	ZipMatch   bool    `json:"zip_match"`
	Score      float64 `json:"score"`
}

// abbrToState maps lowercase state abbreviations to lowercase full names.
var abbrToState = map[string]string{
	"al": "alabama", "ak": "alaska", "az": "arizona", "ar": "arkansas",
	"ca": "california", "co": "colorado", "ct": "connecticut", "de": "delaware",
	"fl": "florida", "ga": "georgia", "hi": "hawaii", "id": "idaho",
	"il": "illinois", "in": "indiana", "ia": "iowa", "ks": "kansas",
	"ky": "kentucky", "la": "louisiana", "me": "maine", "md": "maryland",
	"ma": "massachusetts", "mi": "michigan", "mn": "minnesota", "ms": "mississippi",
	"mo": "missouri", "mt": "montana", "ne": "nebraska", "nv": "nevada",
	"nh": "new hampshire", "nj": "new jersey", "nm": "new mexico", "ny": "new york",
	"nc": "north carolina", "nd": "north dakota", "oh": "ohio", "ok": "oklahoma",
	"or": "oregon", "pa": "pennsylvania", "ri": "rhode island", "sc": "south carolina",
	"sd": "south dakota", "tn": "tennessee", "tx": "texas", "ut": "utah",
	"vt": "vermont", "va": "virginia", "wa": "washington", "wv": "west virginia",
	"wi": "wisconsin", "wy": "wyoming", "dc": "district of columbia",
}

// stateToAbbr maps lowercase full names to lowercase abbreviations.
var stateToAbbr = func() map[string]string {
	m := make(map[string]string, len(abbrToState))
	for abbr, full := range abbrToState {
		m[full] = abbr
	}
	return m
}()

// stateVariants returns both the abbreviation and full name forms for a state.
// Input can be either an abbreviation ("IL") or a full name ("Illinois").
func stateVariants(state string) []string {
	lower := strings.ToLower(strings.TrimSpace(state))
	if lower == "" {
		return nil
	}

	// Check if it's an abbreviation.
	if full, ok := abbrToState[lower]; ok {
		return []string{lower, full}
	}
	// Check if it's a full name.
	if abbr, ok := stateToAbbr[lower]; ok {
		return []string{abbr, lower}
	}
	// Unknown — return as-is.
	return []string{lower}
}

// containsWord checks if text contains needle as a whole word (bounded by
// non-alphanumeric characters or string boundaries). Case-insensitive — both
// text and needle should already be lowercased.
func containsWord(text, needle string) bool {
	if needle == "" || text == "" {
		return false
	}
	start := 0
	for {
		idx := strings.Index(text[start:], needle)
		if idx < 0 {
			return false
		}
		absIdx := start + idx
		endIdx := absIdx + len(needle)

		leftOK := absIdx == 0 || !isAlphaNum(text[absIdx-1])
		rightOK := endIdx == len(text) || !isAlphaNum(text[endIdx])

		if leftOK && rightOK {
			return true
		}
		start = absIdx + 1
	}
}

func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// CrossReferenceAddress checks scraped page content for mentions of the
// company's city, state, and zip code. It returns a match entry per page
// that contains at least one address component.
func CrossReferenceAddress(company model.Company, pages []model.CrawledPage) []AddressMatch {
	if company.City == "" && company.State == "" && company.ZipCode == "" {
		return nil
	}

	var matches []AddressMatch

	for _, page := range pages {
		content := strings.ToLower(page.Markdown)
		if content == "" {
			continue
		}

		var cityMatch, stateMatch, zipMatch bool
		var score float64
		var parts []string

		if company.City != "" && containsWord(content, strings.ToLower(company.City)) {
			cityMatch = true
			score += 0.4
			parts = append(parts, company.City)
		}
		if company.State != "" {
			for _, variant := range stateVariants(company.State) {
				if containsWord(content, variant) {
					stateMatch = true
					score += 0.3
					parts = append(parts, company.State)
					break
				}
			}
		}
		if company.ZipCode != "" && strings.Contains(content, company.ZipCode) {
			zipMatch = true
			score += 0.3
			parts = append(parts, company.ZipCode)
		}

		if score > 0 {
			matches = append(matches, AddressMatch{
				Source:     page.Title,
				Address:    strings.Join(parts, ", "),
				CityMatch:  cityMatch,
				StateMatch: stateMatch,
				ZipMatch:   zipMatch,
				Score:      score,
			})
		}
	}

	return matches
}
