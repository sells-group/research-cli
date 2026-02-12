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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Normalize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
