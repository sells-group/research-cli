package pipeline

import (
	"net/http"
	"testing"

	"github.com/sells-group/research-cli/pkg/jina"
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

func TestValidateJinaResponse_NilResponse(t *testing.T) {
	assert.True(t, ValidateJinaResponse(nil))
}

func TestValidateJinaResponse_NonOKCode(t *testing.T) {
	resp := &jina.ReadResponse{Code: 403}
	assert.True(t, ValidateJinaResponse(resp))
}

func TestValidateJinaResponse_EmptyContent(t *testing.T) {
	resp := &jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{Content: "short"},
	}
	assert.True(t, ValidateJinaResponse(resp))
}

func TestValidateJinaResponse_ChallengeContent(t *testing.T) {
	resp := &jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{Content: "Just a moment... Checking your browser before accessing the site."},
	}
	assert.True(t, ValidateJinaResponse(resp))
}

func TestValidateJinaResponse_ValidContent(t *testing.T) {
	content := "Welcome to Acme Corporation. We are a leading provider of industrial solutions " +
		"with over 50 years of experience. Our headquarters are in New York City."
	resp := &jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{Content: content},
	}
	assert.False(t, ValidateJinaResponse(resp))
}

func TestValidateJinaResponse_ZeroCodeValidContent(t *testing.T) {
	content := "Welcome to Acme Corporation. We are a leading provider of industrial solutions " +
		"with over 50 years of experience. Our headquarters are in New York City."
	resp := &jina.ReadResponse{
		Code: 0,
		Data: jina.ReadData{Content: content},
	}
	assert.False(t, ValidateJinaResponse(resp))
}
