package scraper

import (
	"encoding/json"
	"strconv"
	"strings"
)

// csvColIndex builds a map from header names to column indices.
func csvColIndex(header []string) map[string]int {
	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	return idx
}

// csvString safely extracts a trimmed string from a CSV row by column index.
// Returns empty string if the index is out of range or not found.
func csvString(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// csvFloat64 parses a float64 from a CSV row column, returning 0 on failure.
func csvFloat64(row []string, idx int) float64 {
	s := csvString(row, idx)
	if s == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// csvProperties builds a JSONB byte slice from all columns except excluded ones.
func csvProperties(row []string, header []string, exclude map[string]bool) []byte {
	props := make(map[string]string)
	for i, h := range header {
		h = strings.TrimSpace(h)
		if exclude[h] || i >= len(row) {
			continue
		}
		v := strings.TrimSpace(row[i])
		if v != "" {
			props[h] = v
		}
	}
	data, err := json.Marshal(props)
	if err != nil {
		return []byte("{}")
	}
	return data
}
