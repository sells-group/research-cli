package resolve

import (
	"regexp"
	"strings"
)

// legalSuffixes lists common legal entity suffixes to strip during name normalization.
var legalSuffixes = []string{
	" LLC", " L.L.C.", " L.L.C",
	" INC", " INC.", " INCORPORATED",
	" CORP", " CORP.", " CORPORATION",
	" LTD", " LTD.", " LIMITED",
	" LP", " L.P.", " L.P",
	" LLP", " L.L.P.", " L.L.P",
	" PC", " P.C.", " P.C",
	" PA", " P.A.", " P.A",
	" CO", " CO.",
	" PLC", " P.L.C.",
	" NA", " N.A.", " N.A",
	" DBA", " D/B/A",
	" PLLC",
}

var multiSpaceRe = regexp.MustCompile(`\s{2,}`)

// NormalizeName standardizes an entity name for matching by:
//  1. Trimming whitespace
//  2. Converting to uppercase
//  3. Removing common legal suffixes (LLC, Inc, Corp, etc.)
//  4. Stripping punctuation (commas, periods, dashes, ampersands)
//  5. Collapsing multiple spaces into single spaces
func NormalizeName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}

	name = strings.ToUpper(name)

	// Strip legal suffixes (check longest first is fine since they're all distinct).
	for _, suffix := range legalSuffixes {
		if strings.HasSuffix(name, suffix) {
			name = strings.TrimSuffix(name, suffix)
			break
		}
	}

	// Remove common punctuation.
	name = strings.NewReplacer(
		",", "",
		".", "",
		"'", "",
		"\"", "",
		"&", "AND",
		"-", " ",
	).Replace(name)

	// Collapse multiple spaces.
	name = multiSpaceRe.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)

	return name
}

// NormalizeNameSQL returns a SQL expression that normalizes a column name
// for cross-dataset matching. Applies the same logic as NormalizeName but
// in pure SQL for use in INSERT ... SELECT statements.
func NormalizeNameSQL(col string) string {
	return `UPPER(TRIM(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(` + col + `,
                ',', ''), '.', ''), '''', ''), '"', ''), '&', 'AND'), '-', ' '),
            '\s*(LLC|L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|CORPORATION|LTD\.?|LIMITED|L\.?P\.?|L\.?L\.?P\.?|P\.?C\.?|P\.?A\.?|CO\.?|PLC|P\.?L\.?C\.?|N\.?A\.?|D/?B/?A|PLLC)\s*$',
            '', 'i'),
        '\s+', ' ', 'g')
    ))`
}
