package ppp

import (
	"regexp"
	"strings"
)

var entitySuffixes = regexp.MustCompile(
	`(?i)\s*,?\s*(LLC|L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|CORPORATION|` +
		`CO\.?|COMPANY|LTD\.?|LIMITED|L\.?P\.?|LLP|L\.?L\.?P\.?|` +
		`PLLC|P\.?L\.?L\.?C\.?|P\.?C\.?|DBA|D/B/A)\s*\.?\s*$`)

var multiSpace = regexp.MustCompile(`\s{2,}`)

// Normalize strips entity suffixes and normalizes whitespace for PPP matching.
func Normalize(name string) string {
	n := strings.ToUpper(strings.TrimSpace(name))
	n = entitySuffixes.ReplaceAllString(n, "")
	n = multiSpace.ReplaceAllString(n, " ")
	return strings.TrimSpace(n)
}
