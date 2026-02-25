package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractDomain_Geocode(t *testing.T) {
	// extractDomain is defined in linkedin.go — verify it works for geocode use cases too.
	tests := []struct {
		url      string
		expected string
	}{
		{"https://www.example.com/about", "example.com"},
		{"http://test.com", "test.com"},
		{"https://acme.com/path/to/page", "acme.com"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, extractDomain(tt.url), "url=%s", tt.url)
	}
}
