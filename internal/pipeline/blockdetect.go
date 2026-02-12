package pipeline

import (
	"net/http"
	"strings"

	"github.com/sells-group/research-cli/pkg/jina"
)

// BlockType describes the kind of block detected.
type BlockType string

const (
	BlockNone       BlockType = ""
	BlockCloudflare BlockType = "cloudflare"
	BlockCaptcha    BlockType = "captcha"
	BlockJSShell    BlockType = "js_shell"
)

// DetectBlock checks an HTTP response for signs of anti-bot protection.
func DetectBlock(resp *http.Response, body []byte) (bool, BlockType) {
	if resp == nil {
		return false, BlockNone
	}

	// Cloudflare: 403/503 with cf-* headers.
	if resp.StatusCode == 403 || resp.StatusCode == 503 {
		if resp.Header.Get("cf-ray") != "" || resp.Header.Get("cf-cache-status") != "" {
			return true, BlockCloudflare
		}
		if resp.Header.Get("server") == "cloudflare" {
			return true, BlockCloudflare
		}
	}

	lower := strings.ToLower(string(body))

	// Cloudflare challenge page markers.
	if strings.Contains(lower, "checking your browser") ||
		strings.Contains(lower, "cf-browser-verification") ||
		strings.Contains(lower, "cloudflare") && strings.Contains(lower, "challenge") {
		return true, BlockCloudflare
	}

	// Captcha markers.
	if strings.Contains(lower, "captcha") ||
		strings.Contains(lower, "recaptcha") ||
		strings.Contains(lower, "hcaptcha") {
		return true, BlockCaptcha
	}

	// JS-only shell: very small body with noscript or meta refresh.
	if len(body) < 2000 {
		if strings.Contains(lower, "<noscript") && strings.Contains(lower, "javascript") {
			return true, BlockJSShell
		}
		if strings.Contains(lower, "meta http-equiv=\"refresh\"") {
			return true, BlockJSShell
		}
	}

	return false, BlockNone
}

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
