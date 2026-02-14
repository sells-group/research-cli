package transform

import (
	"strings"
)

// NAICSPrefixes defines the 2-digit NAICS sectors relevant to financial advisory.
// Used to filter large datasets to relevant industries.
var NAICSPrefixes = []string{
	"52", // Finance and Insurance
	"53", // Real Estate
	"54", // Professional, Scientific, and Technical Services
	"55", // Management of Companies and Enterprises
	"56", // Administrative and Support
	"62", // Health Care
	"61", // Educational Services
	"81", // Other Services
}

// IsRelevantNAICS checks if a NAICS code falls within the relevant sectors.
// An empty code is considered relevant (to avoid dropping data).
func IsRelevantNAICS(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" || code == "-" {
		return true
	}
	for _, prefix := range NAICSPrefixes {
		if strings.HasPrefix(code, prefix) {
			return true
		}
	}
	return false
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
