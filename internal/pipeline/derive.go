package pipeline

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"github.com/sells-group/research-cli/internal/model"
)

// DeriveCompanyInfo extracts company name, city, and state from raw HTML.
// Uses a priority chain: og:site_name → og:title → JSON-LD Organization → <title> → domain.
// Zero additional HTTP requests — parses the body already fetched by Probe().
func DeriveCompanyInfo(body []byte, finalURL string) (name, city, state string) {
	html := string(body)

	// 1. og:site_name — cleanest company name source.
	if v := extractMetaContent(html, "og:site_name"); v != "" {
		name = strings.TrimSpace(v)
	}

	// 2. og:title — often has company name.
	if name == "" {
		if v := extractMetaContent(html, "og:title"); v != "" {
			name = cleanTitleToName(v)
		}
	}

	// 3. JSON-LD Organization.
	if name == "" || city == "" {
		jName, jCity, jState := extractJSONLDOrg(html)
		if name == "" && jName != "" {
			name = jName
		}
		if jCity != "" {
			city = jCity
		}
		if jState != "" {
			state = jState
		}
	}

	// 4. <title> tag.
	if name == "" {
		if v := extractTitle(html); v != "" {
			name = cleanTitleToName(v)
		}
	}

	// 5. Domain heuristic as last resort.
	if name == "" {
		name = domainToName(finalURL)
	}

	return name, city, state
}

// deriveNameFromPages extracts a company name from crawled page titles.
// Used as fallback when Phase 0 failed (blocked site that Firecrawl crawled).
func deriveNameFromPages(pages []model.CrawledPage, companyURL string) string {
	if len(pages) == 0 {
		return domainToName(companyURL)
	}
	// Prefer the homepage (URL matches company URL).
	normalized := strings.TrimRight(strings.TrimPrefix(strings.TrimPrefix(companyURL, "https://"), "http://"), "/")
	for _, p := range pages {
		pNorm := strings.TrimRight(strings.TrimPrefix(strings.TrimPrefix(p.URL, "https://"), "http://"), "/")
		if pNorm == normalized || pNorm == "www."+normalized || normalized == "www."+pNorm {
			if cleaned := cleanTitleToName(p.Title); cleaned != "" {
				return cleaned
			}
		}
	}
	// Fallback: first page with a non-empty title.
	for _, p := range pages {
		if cleaned := cleanTitleToName(p.Title); cleaned != "" {
			return cleaned
		}
	}
	return domainToName(companyURL)
}

// domainToName converts a URL's domain to a human-readable company name.
// "acme-construction.com" → "Acme Construction"
func domainToName(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		// Try adding scheme.
		u, err = url.Parse("https://" + rawURL)
		if err != nil || u.Host == "" {
			return ""
		}
	}
	host := strings.TrimPrefix(u.Host, "www.")
	// Strip port.
	if idx := strings.Index(host, ":"); idx > 0 {
		host = host[:idx]
	}
	// Strip TLD.
	parts := strings.Split(host, ".")
	if len(parts) < 2 {
		return ""
	}
	domain := parts[0]
	// Convert hyphens to spaces and title-case.
	words := strings.Split(domain, "-")
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

// DetectInputMode returns the input mode based on populated fields.
func DetectInputMode(company model.Company) model.InputMode {
	if company.Name == "" {
		return model.InputModeURLOnly
	}
	if len(company.PreSeeded) > 0 {
		return model.InputModePreSeeded
	}
	if company.Location != "" || (company.City != "" && company.State != "") {
		return model.InputModeStandard
	}
	return model.InputModeMinimal
}

// --- HTML parsing helpers ---

var metaContentRe = regexp.MustCompile(`(?i)<meta\s[^>]*?(?:property|name)\s*=\s*["']([^"']+)["'][^>]*?content\s*=\s*["']([^"']*?)["']`)
var metaContentRevRe = regexp.MustCompile(`(?i)<meta\s[^>]*?content\s*=\s*["']([^"']*?)["'][^>]*?(?:property|name)\s*=\s*["']([^"']+)["']`)

// extractMetaContent returns the content of a <meta> tag by property or name.
func extractMetaContent(html, name string) string {
	lowerName := strings.ToLower(name)
	for _, m := range metaContentRe.FindAllStringSubmatch(html, -1) {
		if strings.ToLower(m[1]) == lowerName {
			return m[2]
		}
	}
	// Try reversed attribute order: content before property.
	for _, m := range metaContentRevRe.FindAllStringSubmatch(html, -1) {
		if strings.ToLower(m[2]) == lowerName {
			return m[1]
		}
	}
	return ""
}

var titleRe = regexp.MustCompile(`(?i)<title[^>]*>(.*?)</title>`)

// extractTitle returns the text content of the <title> tag.
func extractTitle(html string) string {
	m := titleRe.FindStringSubmatch(html)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}

// titleSuffixes are common trailing patterns stripped from <title> tags.
var titleSuffixes = []string{
	" - Home",
	" | Home",
	" - Homepage",
	" | Homepage",
	" - Official Site",
	" | Official Site",
	" - Official Website",
	" | Official Website",
	" - Welcome",
	" | Welcome",
}

// cleanTitleToName strips common boilerplate from a page title.
func cleanTitleToName(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	for _, suffix := range titleSuffixes {
		if strings.HasSuffix(strings.ToLower(title), strings.ToLower(suffix)) {
			title = title[:len(title)-len(suffix)]
			break
		}
	}
	// If title has " - " or " | " separators, take the first segment.
	for _, sep := range []string{" | ", " - ", " — ", " – "} {
		if idx := strings.Index(title, sep); idx > 0 {
			title = title[:idx]
			break
		}
	}
	return strings.TrimSpace(title)
}

// jsonLDOrg represents a minimal JSON-LD Organization object.
type jsonLDOrg struct {
	Type    string      `json:"@type"`
	Name    string      `json:"name"`
	Address interface{} `json:"address"`
}

// jsonLDAddress represents a JSON-LD PostalAddress.
type jsonLDAddress struct {
	Locality string `json:"addressLocality"`
	Region   string `json:"addressRegion"`
}

var jsonLDRe = regexp.MustCompile(`(?is)<script[^>]*type\s*=\s*["']application/ld\+json["'][^>]*>(.*?)</script>`)

// extractJSONLDOrg extracts name, city, state from JSON-LD Organization markup.
func extractJSONLDOrg(html string) (name, city, state string) {
	for _, m := range jsonLDRe.FindAllStringSubmatch(html, -1) {
		raw := strings.TrimSpace(m[1])
		// Try single object.
		var org jsonLDOrg
		if err := json.Unmarshal([]byte(raw), &org); err == nil {
			if isOrgType(org.Type) {
				name, city, state = extractFromOrg(org)
				if name != "" {
					return name, city, state
				}
			}
		}
		// Try array of objects.
		var arr []jsonLDOrg
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			for _, org := range arr {
				if isOrgType(org.Type) {
					name, city, state = extractFromOrg(org)
					if name != "" {
						return name, city, state
					}
				}
			}
		}
	}
	return "", "", ""
}

func isOrgType(t string) bool {
	lower := strings.ToLower(t)
	return lower == "organization" || lower == "localbusiness" ||
		lower == "corporation" || strings.HasSuffix(lower, "business")
}

func extractFromOrg(org jsonLDOrg) (name, city, state string) {
	name = strings.TrimSpace(org.Name)
	if org.Address == nil {
		return name, "", ""
	}
	// Address can be a string or an object.
	switch addr := org.Address.(type) {
	case map[string]interface{}:
		if v, ok := addr["addressLocality"].(string); ok {
			city = strings.TrimSpace(v)
		}
		if v, ok := addr["addressRegion"].(string); ok {
			state = strings.TrimSpace(v)
		}
	case string:
		// Try "City, ST" format.
		parts := strings.Split(addr, ",")
		if len(parts) >= 2 {
			city = strings.TrimSpace(parts[0])
			state = strings.TrimSpace(parts[len(parts)-1])
		}
	}
	return name, city, state
}
