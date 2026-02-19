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

// parseBoolYN returns true if the string is "Y" (case-insensitive), false otherwise.
func parseBoolYN(s string) bool {
	return strings.EqualFold(strings.TrimSpace(s), "Y")
}

// sanitizeUTF8 replaces invalid UTF-8 byte sequences (e.g., Latin-1 data)
// with empty strings so Postgres doesn't reject the row.
func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, "")
}

// normalizeCol strips parentheses and lowercases for cross-format column matching.
// "5E(1)" → "5e1", "5F(2)(C)" → "5f2c", "Legal Name" → "legal name"
func normalizeCol(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "(", "")
	s = strings.ReplaceAll(s, ")", "")
	return s
}

// mapColumnsNormalized builds a normalized column name → index map.
func mapColumnsNormalized(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, col := range header {
		m[normalizeCol(col)] = i
	}
	return m
}

// getColN gets a column value by normalized name.
func getColN(record []string, colIdx map[string]int, name string) string {
	idx, ok := colIdx[normalizeCol(name)]
	if !ok || idx >= len(record) {
		return ""
	}
	return record[idx]
}

// getColNOrExtra gets a column value by normalized name, checking the main record first
// then falling back to extra columns (e.g., data from a supplementary CSV file).
func getColNOrExtra(record []string, colIdx map[string]int, extra map[string]string, name string) string {
	v := getColN(record, colIdx, name)
	if v != "" {
		return v
	}
	if extra != nil {
		return extra[normalizeCol(name)]
	}
	return ""
}

// anyBoolYN returns true if ANY of the named columns is "Y".
// Used for OR-mapping sub-items (e.g., base file 8A1∨8A2∨8A3 → txn_proprietary_interest).
func anyBoolYN(record []string, colIdx map[string]int, names ...string) bool {
	for _, name := range names {
		if parseBoolYN(getColN(record, colIdx, name)) {
			return true
		}
	}
	return false
}

// firstNonEmpty returns the first non-empty value from the named columns.
// Used for columns with different names between formats (e.g., "legal name" vs "1c").
func firstNonEmpty(record []string, colIdx map[string]int, names ...string) string {
	for _, name := range names {
		v := trimQuotes(getColN(record, colIdx, name))
		if v != "" {
			return v
		}
	}
	return ""
}
