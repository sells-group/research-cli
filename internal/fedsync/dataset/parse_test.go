package dataset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIntOr(t *testing.T) {
	tests := []struct {
		name string
		s    string
		def  int
		want int
	}{
		{"valid", "42", 0, 42},
		{"negative", "-7", 0, -7},
		{"empty", "", 99, 99},
		{"whitespace", "  ", 99, 99},
		{"flag N", "N", 0, 0},
		{"flag S", "S", 0, 0},
		{"flag D", "D", 0, 0},
		{"flag G", "G", 0, 0},
		{"flag H", "H", 5, 5},
		{"flag J", "J", 5, 5},
		{"flag K", "K", 5, 5},
		{"non-numeric", "abc", 10, 10},
		{"float", "3.14", 0, 0},
		{"with spaces", " 123 ", 0, 123},
		{"zero", "0", 99, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseIntOr(tt.s, tt.def)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseInt64Or(t *testing.T) {
	tests := []struct {
		name string
		s    string
		def  int64
		want int64
	}{
		{"valid", "1234567890", 0, 1234567890},
		{"negative", "-100", 0, -100},
		{"empty", "", 99, 99},
		{"flag N", "N", 0, 0},
		{"flag S", "S", 5, 5},
		{"flag D", "D", 0, 0},
		{"flag G", "G", 0, 0},
		{"flag H", "H", 0, 0},
		{"flag J", "J", 0, 0},
		{"flag K", "K", 0, 0},
		{"non-numeric", "xyz", 42, 42},
		{"with spaces", " 999 ", 0, 999},
		{"zero", "0", 99, 0},
		{"large", "9223372036854775807", 0, 9223372036854775807}, // max int64
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseInt64Or(tt.s, tt.def)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseFloat64Or(t *testing.T) {
	tests := []struct {
		name string
		s    string
		def  float64
		want float64
	}{
		{"valid integer", "42", 0, 42.0},
		{"valid float", "3.14", 0, 3.14},
		{"negative", "-1.5", 0, -1.5},
		{"empty", "", 99.9, 99.9},
		{"asterisk", "*", 0, 0},
		{"double asterisk", "**", 0, 0},
		{"hash", "#", 0, 0},
		{"non-numeric", "abc", 1.1, 1.1},
		{"with spaces", " 2.718 ", 0, 2.718},
		{"zero", "0", 99.0, 0},
		{"scientific", "1e5", 0, 100000.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFloat64Or(tt.s, tt.def)
			assert.InDelta(t, tt.want, got, 0.0001)
		})
	}
}

func TestParseInt16Or(t *testing.T) {
	tests := []struct {
		name string
		s    string
		def  int16
		want int16
	}{
		{"valid", "42", 0, 42},
		{"negative", "-5", 0, -5},
		{"empty", "", 99, 99},
		{"non-numeric", "abc", 10, 10},
		{"with spaces", " 7 ", 0, 7},
		{"zero", "0", 99, 0},
		{"max int16", "32767", 0, 32767},
		{"overflow", "99999", 0, 0}, // overflow returns default
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseInt16Or(tt.s, tt.def)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTrimQuotes(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{"no quotes", "hello", "hello"},
		{"double quotes", `"hello"`, "hello"},
		{"with spaces", `  "hello"  `, "hello"},
		{"empty", "", ""},
		{"just quotes", `""`, ""},
		{"single inner quotes", `"he said "hi""`, `he said "hi`},
		{"no closing quote", `"hello`, "hello"},
		{"spaces inside", `" hello "`, " hello "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimQuotes(tt.s)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFirstChar(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{"normal", "hello", "h"},
		{"single char", "A", "A"},
		{"empty", "", ""},
		{"whitespace only", "   ", ""},
		{"leading spaces", "  xy", "x"},
		{"digit", "9abc", "9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := firstChar(tt.s)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMapColumns(t *testing.T) {
	header := []string{"Name", " AGE ", "city", "ZIP_CODE"}
	m := mapColumns(header)

	assert.Equal(t, 0, m["name"])
	assert.Equal(t, 1, m["age"])
	assert.Equal(t, 2, m["city"])
	assert.Equal(t, 3, m["zip_code"])

	// Non-existent column
	_, exists := m["nonexistent"]
	assert.False(t, exists)
}

func TestMapColumns_Empty(t *testing.T) {
	m := mapColumns(nil)
	assert.Empty(t, m)
}

func TestGetCol(t *testing.T) {
	colIdx := map[string]int{"name": 0, "age": 1, "city": 2}
	row := []string{"Alice", "30", "NYC"}

	assert.Equal(t, "Alice", getCol(row, colIdx, "name"))
	assert.Equal(t, "30", getCol(row, colIdx, "age"))
	assert.Equal(t, "NYC", getCol(row, colIdx, "city"))

	// Missing column
	assert.Equal(t, "", getCol(row, colIdx, "zip"))

	// Index out of range
	colIdx["bad"] = 99
	assert.Equal(t, "", getCol(row, colIdx, "bad"))
}

func TestGetCol_CaseInsensitive(t *testing.T) {
	colIdx := map[string]int{"name": 0}
	row := []string{"Bob"}

	// getCol lowercases the name lookup
	assert.Equal(t, "Bob", getCol(row, colIdx, "Name"))
	assert.Equal(t, "Bob", getCol(row, colIdx, "NAME"))
}
