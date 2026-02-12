package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

func TestClassifyPhase_DirectMode(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme"},
		{URL: "https://acme.com/about", Title: "About", Markdown: "About Acme Corp"},
	}

	aiClient := &mockAnthropicClient{}

	// Two pages => direct mode (<=3).
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "homepage", "confidence": 0.95}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Once()

	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeHomepage], 1)
	assert.Len(t, index[model.PageTypeAbout], 1)
	assert.Equal(t, 200, usage.InputTokens)
	assert.Equal(t, 40, usage.OutputTokens)
	aiClient.AssertExpectations(t)
}

func TestClassifyPhase_EmptyPages(t *testing.T) {
	ctx := context.Background()
	aiClient := &mockAnthropicClient{}
	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, nil, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Empty(t, index)
	assert.Equal(t, 0, usage.InputTokens)
}

func TestParseClassification_ValidJSON(t *testing.T) {
	result := parseClassification(`{"page_type": "about", "confidence": 0.92}`)
	assert.Equal(t, model.PageTypeAbout, result.PageType)
	assert.InDelta(t, 0.92, result.Confidence, 0.001)
}

func TestParseClassification_InvalidJSON(t *testing.T) {
	result := parseClassification("not json")
	assert.Equal(t, model.PageTypeOther, result.PageType)
	assert.Equal(t, 0.0, result.Confidence)
}

func TestParseClassification_InvalidPageType(t *testing.T) {
	result := parseClassification(`{"page_type": "nonexistent", "confidence": 0.8}`)
	assert.Equal(t, model.PageTypeOther, result.PageType)
}

func TestParseClassification_WithMarkdownFence(t *testing.T) {
	text := "```json\n{\"page_type\": \"services\", \"confidence\": 0.85}\n```"
	result := parseClassification(text)
	assert.Equal(t, model.PageTypeServices, result.PageType)
	assert.InDelta(t, 0.85, result.Confidence, 0.001)
}
