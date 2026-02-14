package dataset

import (
	"strconv"
	"strings"
)

// parseIntOr parses a string as an integer, returning def if parsing fails or the string is empty/flag.
func parseIntOr(s string, def int) int {
	s = strings.TrimSpace(s)
	if s == "" || s == "N" || s == "S" || s == "D" || s == "G" || s == "H" || s == "J" || s == "K" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// parseInt64Or parses a string as an int64, returning def if parsing fails.
func parseInt64Or(s string, def int64) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "N" || s == "S" || s == "D" || s == "G" || s == "H" || s == "J" || s == "K" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

// parseFloat64Or parses a string as a float64, returning def if parsing fails.
func parseFloat64Or(s string, def float64) float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "*" || s == "**" || s == "#" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

// parseInt16Or parses a string as a int16, returning def if parsing fails.
func parseInt16Or(s string, def int16) int16 {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return def
	}
	return int16(v)
}

// trimQuotes removes surrounding double quotes from a CSV field.
func trimQuotes(s string) string {
	return strings.Trim(strings.TrimSpace(s), `"`)
}

// firstChar returns the first character of s, or empty string if s is empty.
func firstChar(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return string(s[0])
}
