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
		return ""
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
