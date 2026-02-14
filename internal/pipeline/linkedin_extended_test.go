package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCleanJSON_PlainCodeFence(t *testing.T) {
	// Code fence without "json" specifier.
	input := "```\n{\"key\": \"value\"}\n```"
	result := cleanJSON(input)
	assert.Equal(t, `{"key": "value"}`, result)
}

func TestCleanJSON_NoJSON(t *testing.T) {
	result := cleanJSON("no json here")
	assert.Equal(t, "no json here", result)
}

func TestCleanJSON_Empty(t *testing.T) {
	result := cleanJSON("")
	assert.Equal(t, "", result)
}

func TestCleanJSON_NestedBraces(t *testing.T) {
	input := `{"outer": {"inner": "value"}}`
	result := cleanJSON(input)
	assert.Equal(t, `{"outer": {"inner": "value"}}`, result)
}

func TestCleanJSON_LeadingWhitespace(t *testing.T) {
	input := "   \n\n{\"key\": \"value\"}"
	result := cleanJSON(input)
	assert.Equal(t, `{"key": "value"}`, result)
}

func TestIsLinkedInLoginWall_EmptyContent(t *testing.T) {
	assert.True(t, isLinkedInLoginWall(""))
}

func TestIsLinkedInLoginWall_LoginRequired(t *testing.T) {
	padded := "login_required: true. " + "This page requires you to log in to view the full content on LinkedIn platform."
	assert.True(t, isLinkedInLoginWall(padded))
}

func TestIsLinkedInLoginWall_PleaseLogIn(t *testing.T) {
	padded := "please log in to continue. " + "This page requires authentication to view the full content on LinkedIn platform."
	assert.True(t, isLinkedInLoginWall(padded))
}

func TestIsLinkedInLoginWall_SignUpToView(t *testing.T) {
	padded := "Sign up to view the full profile. " + "This page requires authentication to view the full content on LinkedIn platform."
	assert.True(t, isLinkedInLoginWall(padded))
}

func TestBuildLinkedInURL_CorpSuffix(t *testing.T) {
	result := buildLinkedInURL("Big Corp")
	assert.Equal(t, "https://www.linkedin.com/company/big", result)
}

func TestBuildLinkedInURL_LtdSuffix(t *testing.T) {
	result := buildLinkedInURL("Small Co Ltd")
	// -co and -ltd are both stripped as entity suffixes.
	assert.Equal(t, "https://www.linkedin.com/company/small", result)
}

func TestBuildLinkedInURL_Trimmed(t *testing.T) {
	result := buildLinkedInURL("  Trimmed  ")
	assert.Equal(t, "https://www.linkedin.com/company/trimmed", result)
}
