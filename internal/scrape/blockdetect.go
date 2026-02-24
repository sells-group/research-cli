package scrape

import (
	"net/http"
	"strings"
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
