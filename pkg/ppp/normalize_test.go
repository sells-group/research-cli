package ppp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "strip LLC",
			input:    "Acme Industrial LLC",
			expected: "ACME INDUSTRIAL",
		},
		{
			name:     "strip INC with period and comma",
			input:    "SMITH & SONS, INC.",
			expected: "SMITH & SONS",
		},
		{
			name:     "strip dotted LLC",
			input:    "Bob's L.L.C.",
			expected: "BOB'S",
		},
		{
			name:     "no suffix to strip",
			input:    "Simple Name",
			expected: "SIMPLE NAME",
		},
		{
			name:     "extra spaces and suffix",
			input:    "  Extra   Spaces  Corp  ",
			expected: "EXTRA SPACES",
		},
		{
			name:     "already clean",
			input:    "Already Clean",
			expected: "ALREADY CLEAN",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "strip CORPORATION",
			input:    "Global Partners Corporation",
			expected: "GLOBAL PARTNERS",
		},
		{
			name:     "strip INCORPORATED",
			input:    "Test Services Incorporated",
			expected: "TEST SERVICES",
		},
		{
			name:     "strip LTD",
			input:    "British Holdings Ltd.",
			expected: "BRITISH HOLDINGS",
		},
		{
			name:     "strip LIMITED",
			input:    "Asia Pacific Limited",
			expected: "ASIA PACIFIC",
		},
		{
			name:     "strip LP",
			input:    "Investment Fund LP",
			expected: "INVESTMENT FUND",
		},
		{
			name:     "strip LLP",
			input:    "Law Firm LLP",
			expected: "LAW FIRM",
		},
		{
			name:     "strip PLLC",
			input:    "Medical Group PLLC",
			expected: "MEDICAL GROUP",
		},
		{
			name:     "strip DBA",
			input:    "Real Name DBA",
			expected: "REAL NAME",
		},
		{
			name:     "strip D/B/A",
			input:    "Real Name D/B/A",
			expected: "REAL NAME",
		},
		{
			name:     "strip COMPANY",
			input:    "Widget Company",
			expected: "WIDGET",
		},
		{
			name:     "strip CO",
			input:    "Old Co.",
			expected: "OLD",
		},
		{
			name:     "strip P.C.",
			input:    "Dental Practice P.C.",
			expected: "DENTAL PRACTICE",
		},
		{
			name:     "case insensitive suffix",
			input:    "lowercase llc",
			expected: "LOWERCASE",
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: "",
		},
		{
			name:     "suffix only",
			input:    "LLC",
			expected: "",
		},
		{
			name:     "name with ampersand and suffix",
			input:    "Smith & Wesson, Inc.",
			expected: "SMITH & WESSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Normalize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
