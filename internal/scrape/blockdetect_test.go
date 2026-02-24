package scrape

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectBlock_Cloudflare403(t *testing.T) {
	resp := &http.Response{
		StatusCode: 403,
		Header:     http.Header{"Cf-Ray": {"abc123"}},
	}
	blocked, bt := DetectBlock(resp, nil)
	assert.True(t, blocked)
	assert.Equal(t, BlockCloudflare, bt)
}

func TestDetectBlock_Cloudflare503Server(t *testing.T) {
	resp := &http.Response{
		StatusCode: 503,
		Header:     http.Header{"Server": {"cloudflare"}},
	}
	blocked, bt := DetectBlock(resp, nil)
	assert.True(t, blocked)
	assert.Equal(t, BlockCloudflare, bt)
}

func TestDetectBlock_CaptchaInBody(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
	}
	body := []byte("<html><body>Please complete the reCAPTCHA to continue</body></html>")
	blocked, bt := DetectBlock(resp, body)
	assert.True(t, blocked)
	assert.Equal(t, BlockCaptcha, bt)
}

func TestDetectBlock_JSShell(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
	}
	body := []byte("<html><noscript>Enable JavaScript to continue</noscript></html>")
	blocked, bt := DetectBlock(resp, body)
	assert.True(t, blocked)
	assert.Equal(t, BlockJSShell, bt)
}

func TestDetectBlock_NilResponse(t *testing.T) {
	blocked, bt := DetectBlock(nil, nil)
	assert.False(t, blocked)
	assert.Equal(t, BlockNone, bt)
}

func TestDetectBlock_CleanPage(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
	}
	body := []byte("<html><body>Welcome to Acme Corp. We build great products.</body></html>")
	blocked, bt := DetectBlock(resp, body)
	assert.False(t, blocked)
	assert.Equal(t, BlockNone, bt)
}
