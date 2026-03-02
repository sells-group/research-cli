package pipeline

import "strings"

// StateToRegion maps a 2-letter US state abbreviation to its Census Bureau
// region: Northeast, Midwest, South, or West. Returns empty string for
// unrecognized inputs.
func StateToRegion(stateAbbr string) string {
	abbr := strings.ToUpper(strings.TrimSpace(stateAbbr))
	if r, ok := regionMap[abbr]; ok {
		return r
	}
	return ""
}

// regionMap maps all 50 states + DC to Census Bureau 4-region classification.
var regionMap = map[string]string{
	// Northeast
	"CT": "Northeast",
	"ME": "Northeast",
	"MA": "Northeast",
	"NH": "Northeast",
	"RI": "Northeast",
	"VT": "Northeast",
	"NJ": "Northeast",
	"NY": "Northeast",
	"PA": "Northeast",

	// Midwest
	"IL": "Midwest",
	"IN": "Midwest",
	"MI": "Midwest",
	"OH": "Midwest",
	"WI": "Midwest",
	"IA": "Midwest",
	"KS": "Midwest",
	"MN": "Midwest",
	"MO": "Midwest",
	"NE": "Midwest",
	"ND": "Midwest",
	"SD": "Midwest",

	// South
	"DE": "South",
	"FL": "South",
	"GA": "South",
	"MD": "South",
	"NC": "South",
	"SC": "South",
	"VA": "South",
	"DC": "South",
	"WV": "South",
	"AL": "South",
	"KY": "South",
	"MS": "South",
	"TN": "South",
	"AR": "South",
	"LA": "South",
	"OK": "South",
	"TX": "South",

	// West
	"AZ": "West",
	"CO": "West",
	"ID": "West",
	"MT": "West",
	"NV": "West",
	"NM": "West",
	"UT": "West",
	"WY": "West",
	"AK": "West",
	"CA": "West",
	"HI": "West",
	"OR": "West",
	"WA": "West",
}
