package transform

import (
	"fmt"
	"strings"
)

// NormalizeFIPSState normalizes a state FIPS code to 2 digits with zero-padding.
func NormalizeFIPSState(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return ""
	}
	if len(code) == 1 {
		return "0" + code
	}
	return code
}

// NormalizeFIPSCounty normalizes a county FIPS code to 3 digits with zero-padding.
func NormalizeFIPSCounty(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return "000"
	}
	for len(code) < 3 {
		code = "0" + code
	}
	return code
}

// CombineFIPS combines state and county FIPS codes into a 5-digit code.
func CombineFIPS(state, county string) string {
	s := NormalizeFIPSState(state)
	c := NormalizeFIPSCounty(county)
	if s == "" || c == "" {
		return ""
	}
	return s + c
}

// FormatFIPS formats a numeric FIPS code with proper zero-padding.
func FormatFIPS(code int, digits int) string {
	return fmt.Sprintf("%0*d", digits, code)
}

// StateAbbrToFIPS maps US state/territory 2-letter abbreviations to FIPS codes.
var StateAbbrToFIPS = map[string]string{
	"AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
	"CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
	"GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
	"IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
	"MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
	"MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
	"NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
	"OH": "39", "OK": "40", "OR": "41", "PA": "42", "PR": "72",
	"RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
	"UT": "49", "VT": "50", "VA": "51", "VI": "78", "WA": "53",
	"WV": "54", "WI": "55", "WY": "56",
}
