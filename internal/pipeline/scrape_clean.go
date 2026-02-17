package pipeline

import "strings"

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
