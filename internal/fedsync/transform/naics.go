package transform

import (
	"strings"
)

// NAICSPrefixes defines the 2-digit NAICS sectors used by QCEW file filtering.
var NAICSPrefixes = []string{
	"10", // Total (aggregate)
	"11", // Agriculture, Forestry, Fishing and Hunting
	"21", // Mining, Quarrying, and Oil and Gas Extraction
	"22", // Utilities
	"23", // Construction
	"31", // Manufacturing
	"32", // Manufacturing
	"33", // Manufacturing
	"42", // Wholesale Trade
	"44", // Retail Trade
	"45", // Retail Trade
	"48", // Transportation and Warehousing
	"49", // Transportation and Warehousing
	"51", // Information
	"52", // Finance and Insurance
	"53", // Real Estate
	"54", // Professional, Scientific, and Technical Services
	"55", // Management of Companies and Enterprises
	"56", // Administrative and Support
	"61", // Educational Services
	"62", // Health Care
	"71", // Arts, Entertainment, and Recreation
	"72", // Accommodation and Food Services
	"81", // Other Services
	"92", // Public Administration
}

// IsRelevantNAICS returns true for any non-empty NAICS code.
// All sectors are relevant for revenue estimation and market sizing.
func IsRelevantNAICS(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" || code == "-" {
		return true
	}
	return true
}

// NormalizeNAICS normalizes a NAICS code to 6 digits by padding with zeros.
// Returns the original if it's longer than 6 digits or empty.
func NormalizeNAICS(code string) string {
	code = strings.TrimSpace(code)
	if code == "" || code == "-" {
		return ""
	}
	// Strip any trailing dashes (e.g., "5221--" → "5221")
	code = strings.TrimRight(code, "-")
	if len(code) > 6 {
		return code
	}
	for len(code) < 6 {
		code += "0"
	}
	return code
}

// NAICSToSector returns the 2-digit sector code.
func NAICSToSector(code string) string {
	code = strings.TrimSpace(code)
	if len(code) < 2 {
		return ""
	}
	return code[:2]
}
