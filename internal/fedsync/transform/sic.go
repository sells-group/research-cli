package transform

import "strings"

// SICToNAICS provides a simplified SIC→NAICS crosswalk for the most common codes.
// Full crosswalk is in the database (fed_data.sic_crosswalk), but this is useful
// for quick lookups without DB access.
var SICToNAICS = map[string]string{
	"6021": "522110", // National Commercial Banks
	"6022": "522110", // State Commercial Banks
	"6035": "522120", // Savings Institutions, Federally Chartered
	"6036": "522120", // Savings Institutions, Not Federally Chartered
	"6099": "522390", // Services Allied with the Exchange of Securities
	"6111": "522310", // Federal & Federally-Sponsored Credit Agencies
	"6141": "522210", // Personal Credit Institutions
	"6153": "522220", // Short-Term Business Credit Institutions
	"6159": "522293", // Miscellaneous Business Credit Institutions
	"6162": "522292", // Mortgage Bankers, Loan Correspondents
	"6163": "522310", // Loan Brokers
	"6211": "523110", // Security Brokers, Dealers, and Flotation Companies
	"6282": "523930", // Investment Advice
	"6311": "524113", // Life Insurance
	"6321": "524126", // Accident and Health Insurance
	"6331": "524126", // Fire, Marine & Casualty Insurance
	"6411": "524210", // Insurance Agents, Brokers and Service
	"6512": "531110", // Operators of Apartment Buildings
	"6531": "531210", // Real Estate Agents and Managers
	"6552": "531390", // Land Subdividers and Developers
	"6726": "525910", // Investment Offices, Not Elsewhere Classified
	"6732": "813211", // Educational, Religious, Charitable Trusts
	"6733": "525920", // Trusts, Except Educational, Religious
	"6798": "525930", // Real Estate Investment Trusts
	"6799": "523910", // Investors, Not Elsewhere Classified
}

// NormalizeSIC normalizes a SIC code to 4 digits with zero-padding.
func NormalizeSIC(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return ""
	}
	for len(code) < 4 {
		code = "0" + code
	}
	return code
}

// SICLookupNAICS returns the NAICS code for a given SIC code using the built-in crosswalk.
// Returns empty string if not found.
func SICLookupNAICS(sic string) string {
	return SICToNAICS[NormalizeSIC(sic)]
}
