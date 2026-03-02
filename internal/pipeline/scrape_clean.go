package pipeline

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/sells-group/research-cli/internal/model"
)

// CleanExternalMarkdown strips boilerplate from external source pages,
// keeping only the business-relevant content.
func CleanExternalMarkdown(source, md string) string {
	switch source {
	case "bbb":
		return cleanBBBMarkdown(md)
	case "google_maps":
		return cleanGoogleMapsMarkdown(md)
	default:
		return md
	}
}

// cleanBBBMarkdown removes BBB boilerplate sections that bury actual
// business data (cookie consent, navigation, footer, sidebar content).
func cleanBBBMarkdown(md string) string {
	// Strip everything from the first cookie/consent marker through the last
	// "Accept All Cookies" line. This catches both the initial cookie banner
	// AND the cookie preferences modal (Necessary/Functional/Performance/Marketing).
	for {
		idx := strings.Index(md, "Accept All Cookies")
		if idx < 0 {
			break
		}
		// Find the start: look back for known cookie section openers.
		start := idx
		for _, marker := range []string{"Cookies on BBB.org", "Cookie Preferences", "cookie"} {
			if s := strings.LastIndex(md[:idx], marker); s >= 0 && s < start {
				start = s
			}
		}
		// Also look back for [Skip to main content] which precedes the cookie block.
		if s := strings.LastIndex(md[:idx], "[Skip to main content]"); s >= 0 {
			// Only use if it's close (within 200 chars of cookie start).
			if start-s < 200 {
				start = s
			}
		}
		end := idx + len("Accept All Cookies")
		if nlIdx := strings.Index(md[end:], "\n"); nlIdx >= 0 {
			end += nlIdx + 1
		}
		md = md[:start] + md[end:]
	}

	// Strip BBB header navigation block (between "Better Business Bureau" and
	// the standalone "Business Profile" heading). This catches the
	// Consumers/Businesses/Scam Tracker nav links, language selector, and
	// search forms.
	// Use "\nBusiness Profile\n" to target a line-start heading, not the title.
	if bpIdx := strings.Index(md, "\nBusiness Profile\n"); bpIdx > 0 {
		bpIdx++ // skip the leading newline to point at "Business Profile"
		// Look back for the BBB header that starts the nav block.
		for _, marker := range []string{
			"Better Business Bureau",
			"Powered by data",
		} {
			if hdrIdx := strings.LastIndex(md[:bpIdx], marker); hdrIdx >= 0 {
				// Find the start of that line.
				lineStart := strings.LastIndex(md[:hdrIdx], "\n")
				if lineStart < 0 {
					lineStart = 0
				}
				md = md[:lineStart] + "\n" + md[bpIdx:]
				break
			}
		}
	}

	// Strip "Table of Contents" section — it's just internal links.
	md = stripSection(md, "Table of Contents")

	// Strip navigation menu.
	if navStart := strings.Index(md, "Navigation menu"); navStart >= 0 {
		// Look for a reasonable end marker (breadcrumb or first heading after nav).
		rest := md[navStart:]
		navEnd := -1
		for _, marker := range []string{"Breadcrumb", "# ", "Business Profile"} {
			if idx := strings.Index(rest, marker); idx > 0 {
				if navEnd < 0 || idx < navEnd {
					navEnd = idx
				}
			}
		}
		if navEnd > 0 {
			md = md[:navStart] + md[navStart+navEnd:]
		}
	}

	// Strip footer.
	for _, footerMarker := range []string{
		"BBB Business Profiles may not be reproduced",
		"BBB Serving",
		"File a Complaint",
		"For the best experience",
	} {
		if idx := strings.Index(md, footerMarker); idx >= 0 {
			// Find the start of the footer section — look for preceding separator.
			cutPoint := idx
			if sepIdx := strings.LastIndex(md[:idx], "---"); sepIdx >= 0 && idx-sepIdx < 200 {
				cutPoint = sepIdx
			}
			md = md[:cutPoint]
			break
		}
	}

	// Strip known sidebar sections that add noise.
	for _, section := range []string{
		"Industry Tip",
		"More Resources",
		"Featured Content",
		"Related Categories",
		"BBB Reports On",
	} {
		md = stripSection(md, section)
	}

	return strings.TrimSpace(md)
}

// cleanGoogleMapsMarkdown strips the Google Maps sign-in/promo footer.
func cleanGoogleMapsMarkdown(md string) string {
	for _, marker := range []string{
		"Get the most out of Google Maps",
		"Sign in to Google Maps",
		"Sign in\n\nGet the",
	} {
		if idx := strings.Index(md, marker); idx >= 0 {
			md = md[:idx]
			break
		}
	}

	return strings.TrimSpace(md)
}

// reviewPattern defines a regex pattern for extracting review data, specifying
// which capture groups hold the rating and review count.
type reviewPattern struct {
	re          *regexp.Regexp
	ratingGroup int
	countGroup  int
}

// Review/rating regex patterns.
var (
	// googleMapsReviewPatterns are tried in order; first match wins.
	googleMapsReviewPatterns = []reviewPattern{
		// "4.8 stars (127 reviews)", "4.5 stars · 89 reviews", "3.0 star (1 review)"
		{regexp.MustCompile(`(\d+\.?\d*)\s+stars?\s*(?:[\(·]\s*)?(\d[\d,]*)\s+reviews?\)?`), 1, 2},
		// Markdown bold rating: "**4.8** (127 reviews)"
		{regexp.MustCompile(`\*\*(\d+\.?\d*)\*\*\s*\(?(\d[\d,]*)\s+reviews?\)?`), 1, 2},
		// Slash-5 format: "4.8/5 (127 reviews)"
		{regexp.MustCompile(`(\d+\.?\d*)/5\s*\(?(\d[\d,]*)\s+reviews?\)?`), 1, 2},
		// "Rated 4.8 out of 5 · 127 reviews"
		{regexp.MustCompile(`[Rr]ated\s+(\d+\.?\d*)\s+out\s+of\s+5\s*[·\-]\s*(\d[\d,]*)\s+reviews?`), 1, 2},
		// Plain adjacent (most permissive): "4.8 127 reviews"
		{regexp.MustCompile(`(\d+\.\d+)\s+(\d[\d,]*)\s+reviews?`), 1, 2},
	}

	// BBB rating: "BBB Rating: A+", "BBB Rating A-", "BBB Rating: F"
	bbbRatingRe = regexp.MustCompile(`BBB\s+Rating:?\s+([A-F][+-]?)`)
)

// ParseReviewMetadata extracts structured review/rating data from external
// source markdown. Returns nil if no metadata is found.
func ParseReviewMetadata(source, md string) *model.PageMetadata {
	switch source {
	case "google_maps":
		return parseGoogleMapsMetadata(md)
	case "bbb", "bbb_profile":
		return parseBBBMetadata(md)
	default:
		return nil
	}
}

func parseGoogleMapsMetadata(md string) *model.PageMetadata {
	for _, p := range googleMapsReviewPatterns {
		m := p.re.FindStringSubmatch(md)
		if m == nil {
			continue
		}
		rating, err := strconv.ParseFloat(m[p.ratingGroup], 64)
		if err != nil {
			continue
		}
		// Bounds validation: Google ratings are 1.0–5.0.
		if rating < 1.0 || rating > 5.0 {
			continue
		}
		countStr := strings.ReplaceAll(m[p.countGroup], ",", "")
		count, err := strconv.Atoi(countStr)
		if err != nil {
			continue
		}
		return &model.PageMetadata{
			Rating:      rating,
			ReviewCount: count,
			Source:      "regex",
		}
	}
	return nil
}

func parseBBBMetadata(md string) *model.PageMetadata {
	m := bbbRatingRe.FindStringSubmatch(md)
	if m == nil {
		return nil
	}
	return &model.PageMetadata{
		BBBRating: m[1],
	}
}

// stripSection removes a named section and its content (up to the next
// heading or separator) from the markdown.
func stripSection(md, sectionTitle string) string {
	idx := strings.Index(md, sectionTitle)
	if idx < 0 {
		return md
	}

	// Find the start of this section's heading line.
	start := idx
	if lineStart := strings.LastIndex(md[:idx], "\n"); lineStart >= 0 {
		start = lineStart
	}

	// Find the end: next heading (# or ##) or separator (---) or end of string.
	rest := md[idx+len(sectionTitle):]
	end := len(rest)
	for _, marker := range []string{"\n# ", "\n## ", "\n---"} {
		if markerIdx := strings.Index(rest, marker); markerIdx >= 0 && markerIdx < end {
			end = markerIdx
		}
	}

	return md[:start] + md[idx+len(sectionTitle)+end:]
}

// Phone regex: matches tel: links and US phone patterns in markdown.
var (
	telLinkRe = regexp.MustCompile(`\[.*?\]\(tel:(\+?[\d-]+)\)`)
	phoneRe   = regexp.MustCompile(`(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}`)
)

// ParsePhoneFromMarkdown extracts the first US phone number from page markdown.
// Prefers tel: link hrefs (most reliable) over inline text patterns.
// When multiple inline matches exist, prefers numbers near "phone"/"call"/"contact"
// keywords and deprioritizes numbers near "fax"/"toll-free"/"support".
func ParsePhoneFromMarkdown(md string) string {
	// Priority 1: tel: links
	if m := telLinkRe.FindStringSubmatch(md); m != nil {
		digits := normalizePhoneDigits(m[1])
		if len(digits) >= 10 {
			return digits
		}
	}
	// Priority 2: inline phone patterns (skip if too many matches — likely a directory)
	matches := phoneRe.FindAllStringIndex(md, 10)
	if len(matches) < 1 || len(matches) > 5 {
		return ""
	}
	if len(matches) == 1 {
		digits := normalizePhoneDigits(md[matches[0][0]:matches[0][1]])
		if len(digits) >= 10 {
			return digits
		}
		return ""
	}
	// Multiple matches: score by keyword proximity.
	best, bestScore := "", -1000
	for _, idx := range matches {
		digits := normalizePhoneDigits(md[idx[0]:idx[1]])
		if len(digits) < 10 {
			continue
		}
		score := phoneContextScore(md, idx[0], idx[1])
		if best == "" || score > bestScore {
			best = digits
			bestScore = score
		}
	}
	return best
}

// phoneContextScore scores a phone match position by surrounding keyword context.
// Positive keywords (phone, call, contact, tel) boost; negative keywords
// (fax, toll-free, support) penalize. Searches ±100 chars around the match.
func phoneContextScore(md string, start, end int) int {
	lo := start - 100
	if lo < 0 {
		lo = 0
	}
	hi := end + 100
	if hi > len(md) {
		hi = len(md)
	}
	ctx := strings.ToLower(md[lo:hi])
	score := 0
	for _, kw := range []string{"phone", "call ", "tel:", "contact", "main"} {
		if strings.Contains(ctx, kw) {
			score++
		}
	}
	for _, kw := range []string{"fax", "toll-free", "toll free", "support", "helpline"} {
		if strings.Contains(ctx, kw) {
			score--
		}
	}
	return score
}

func normalizePhoneDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
