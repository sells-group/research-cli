package pipeline

import (
	"strings"

	"github.com/sells-group/research-cli/pkg/jina"
)

// ValidateJinaResponse checks whether a Jina response contains usable content
// or indicates the page is blocked/empty. Returns true if the response needs
// a Firecrawl fallback.
func ValidateJinaResponse(resp *jina.ReadResponse) bool {
	if resp == nil {
		return true
	}

	// Non-200 code from Jina.
	if resp.Code != 0 && resp.Code != 200 {
		return true
	}

	content := strings.TrimSpace(resp.Data.Content)

	// Empty or near-empty content.
	if len(content) < 100 {
		return true
	}

	lower := strings.ToLower(content)

	// Challenge page signatures in markdown.
	challengeSignatures := []string{
		"checking your browser",
		"enable javascript",
		"please enable cookies",
		"access denied",
		"403 forbidden",
		"just a moment",
		"cloudflare",
		"attention required",
	}

	for _, sig := range challengeSignatures {
		if strings.Contains(lower, sig) && len(content) < 1000 {
			return true
		}
	}

	return false
}
