// Package pipeline orchestrates the multi-phase company enrichment workflow.
package pipeline

import (
	"regexp"
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

// Compiled regexps for structured address extraction.
var (
	// cityStateZipRe matches "City, ST 12345" or "City, ST 12345-6789".
	// Uses [^\S\n] instead of \s to avoid matching newlines in city names.
	cityStateZipRe = regexp.MustCompile(`(?m)([\w][\w ]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)`)

	// streetRe matches a line starting with a street number (e.g., "123 Main St").
	streetRe = regexp.MustCompile(`(?m)^(\d+\s+.+)$`)

	// embeddedStreetRe extracts a street address embedded within a longer line
	// (e.g., "located at 500 Market Street").
	embeddedStreetRe = regexp.MustCompile(`(\d+\s+[\w][\w\s]*(?:Street|St|Avenue|Ave|Boulevard|Blvd|Drive|Dr|Road|Rd|Lane|Ln|Way|Place|Pl|Court|Ct|Circle|Cir|Parkway|Pkwy|Highway|Hwy|Trail|Trl)\.?)`)

	// sectionHeaderRe matches BBB/SoS address section headers.
	sectionHeaderRe = regexp.MustCompile(`(?mi)(?:business\s+address|^address|principal\s+office|registered\s+agent)`)
)

// ExtractStructuredAddress attempts to parse a structured US address from
// BBB or SoS page markdown using regex patterns. Returns the parsed
// components and true if a valid address was found.
func ExtractStructuredAddress(markdown, _ string) (street, city, state, zip string, ok bool) {
	if markdown == "" {
		return "", "", "", "", false
	}

	// Find city/state/zip matches.
	cszMatches := cityStateZipRe.FindAllStringSubmatchIndex(markdown, -1)
	if len(cszMatches) == 0 {
		return "", "", "", "", false
	}

	// Prefer a match near a section header if one exists.
	headerLoc := sectionHeaderRe.FindStringIndex(markdown)

	bestIdx := 0
	if headerLoc != nil && len(cszMatches) > 1 {
		bestDist := len(markdown)
		for i, m := range cszMatches {
			dist := m[0] - headerLoc[1]
			if dist < 0 {
				dist = -dist
			}
			if dist < bestDist {
				bestDist = dist
				bestIdx = i
			}
		}
	}

	m := cszMatches[bestIdx]
	city = strings.TrimSpace(markdown[m[2]:m[3]])
	state = markdown[m[4]:m[5]]
	zip = markdown[m[6]:m[7]]

	// Look for a street line in the text preceding the city/state/zip match.
	// Search up to 200 chars before the match for the street.
	searchStart := m[0] - 200
	if searchStart < 0 {
		searchStart = 0
	}
	preceding := markdown[searchStart:m[0]]

	// Split preceding text into lines and search backwards for street lines.
	lines := strings.Split(preceding, "\n")
	var streetParts []string
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		// Skip empty lines and markdown formatting.
		if line == "" || line == "---" {
			if len(streetParts) > 0 {
				break // Stop at a gap if we already found street parts.
			}
			continue
		}
		// Remove markdown bold markers.
		line = strings.ReplaceAll(line, "**", "")
		line = strings.TrimSpace(line)

		// If this looks like a section header, stop.
		if sectionHeaderRe.MatchString(line) {
			break
		}

		// Check if this line looks like a street address or suite/floor.
		if streetRe.MatchString(line) || isSuiteLine(line) {
			streetParts = append([]string{line}, streetParts...)
		} else if len(streetParts) > 0 {
			break // Non-street line after collecting street parts — stop.
		} else if em := embeddedStreetRe.FindString(line); em != "" {
			// Fallback: extract street address embedded within a sentence.
			streetParts = append([]string{em}, streetParts...)
		}
	}

	if len(streetParts) > 0 {
		street = strings.Join(streetParts, ", ")
	}

	return street, city, state, zip, true
}

// isSuiteLine checks if a line is a suite/floor/unit continuation of a street address.
func isSuiteLine(line string) bool {
	lower := strings.ToLower(line)
	prefixes := []string{"suite ", "ste ", "ste. ", "#", "unit ", "floor ", "fl ", "apt ", "bldg ", "building "}
	for _, p := range prefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
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
