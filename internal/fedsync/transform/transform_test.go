package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRelevantNAICS(t *testing.T) {
	tests := []struct {
		code     string
		relevant bool
	}{
		{"523110", true}, // Finance
		{"541110", true}, // Professional services
		{"311111", true}, // Food manufacturing
		{"236115", true}, // Construction
		{"", true},       // Empty = include
		{"-", true},      // Dash = include
		{"52", true},     // Sector-level
	}
	for _, tt := range tests {
		assert.Equal(t, tt.relevant, IsRelevantNAICS(tt.code), "code: %q", tt.code)
	}
}

func TestNormalizeNAICS(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{"5221", "522100"},
		{"522110", "522110"},
		{"52", "520000"},
		{"", ""},
		{"-", ""},
		{"5221--", "522100"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, NormalizeNAICS(tt.input), "input: %q", tt.input)
	}
}

func TestNAICSToSector(t *testing.T) {
	assert.Equal(t, "52", NAICSToSector("522110"))
	assert.Equal(t, "54", NAICSToSector("54"))
	assert.Equal(t, "", NAICSToSector("5"))
	assert.Equal(t, "", NAICSToSector(""))
}

func TestNormalizeFIPSState(t *testing.T) {
	assert.Equal(t, "06", NormalizeFIPSState("6"))
	assert.Equal(t, "06", NormalizeFIPSState("06"))
	assert.Equal(t, "36", NormalizeFIPSState("36"))
	assert.Equal(t, "", NormalizeFIPSState(""))
}

func TestNormalizeFIPSCounty(t *testing.T) {
	assert.Equal(t, "001", NormalizeFIPSCounty("1"))
	assert.Equal(t, "037", NormalizeFIPSCounty("37"))
	assert.Equal(t, "037", NormalizeFIPSCounty("037"))
	assert.Equal(t, "000", NormalizeFIPSCounty("")) // state-level defaults to "000"
}

func TestCombineFIPS(t *testing.T) {
	assert.Equal(t, "06037", CombineFIPS("6", "37"))
	assert.Equal(t, "36061", CombineFIPS("36", "061"))
	assert.Equal(t, "", CombineFIPS("", "037"))
	assert.Equal(t, "06000", CombineFIPS("06", "")) // state-level: county defaults to "000"
}

func TestFormatFIPS(t *testing.T) {
	assert.Equal(t, "06", FormatFIPS(6, 2))
	assert.Equal(t, "037", FormatFIPS(37, 3))
	assert.Equal(t, "06037", FormatFIPS(6037, 5))
}

func TestNormalizeSIC(t *testing.T) {
	assert.Equal(t, "6211", NormalizeSIC("6211"))
	assert.Equal(t, "0111", NormalizeSIC("111"))
	assert.Equal(t, "", NormalizeSIC(""))
}

func TestSICLookupNAICS(t *testing.T) {
	assert.Equal(t, "523110", SICLookupNAICS("6211"))
	assert.Equal(t, "523930", SICLookupNAICS("6282"))
	assert.Equal(t, "", SICLookupNAICS("9999"))
}
