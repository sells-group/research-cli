package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/stretchr/testify/assert"
)

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
