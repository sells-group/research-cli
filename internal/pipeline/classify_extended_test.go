package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

// testPageContent returns content >= 100 chars to bypass the tiny page filter.
func testPageContent(i int) string {
	return fmt.Sprintf("Content for page %d. %s", i, strings.Repeat("This is substantial content for testing purposes. ", 3))
}

func TestClassifyPhase_GroupedMode(t *testing.T) {
	ctx := context.Background()

	// 5 pages with unique content >= 100 chars: triggers grouped classification (> 3 pages).
	pages := make([]model.CrawledPage, 5)
	for i := 0; i < 5; i++ {
		pages[i] = model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/page%d", i),
			Title:    fmt.Sprintf("Page %d", i),
			Markdown: testPageContent(i),
		}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Grouped classification sends all 5 pages in 1 call (pagesPerGroup=8).
	pageTypes := []string{"homepage", "about", "services", "contact", "other"}
	var jsonArray string
	for i := 0; i < 5; i++ {
		if i > 0 {
			jsonArray += ","
		}
		jsonArray += fmt.Sprintf(`{"url":"https://acme.com/page%d","page_type":"%s","confidence":0.9}`, i, pageTypes[i])
	}

	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "[" + jsonArray + "]"}},
			Usage:   anthropic.TokenUsage{InputTokens: 250, OutputTokens: 50},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.NotEmpty(t, index)
	assert.Equal(t, 250, usage.InputTokens)
	assert.Equal(t, 50, usage.OutputTokens)
	aiClient.AssertExpectations(t)
}

func TestClassifyPhase_GroupedMode_APIError(t *testing.T) {
	ctx := context.Background()

	pages := make([]model.CrawledPage, 5)
	for i := 0; i < 5; i++ {
		pages[i] = model.CrawledPage{URL: fmt.Sprintf("https://acme.com/page%d", i), Markdown: testPageContent(i)}
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Grouped mode falls back to "other" on error instead of returning an error.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(nil, errors.New("rate limited")).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, _, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	// Grouped mode gracefully degrades to "other" on error, no hard failure.
	assert.NoError(t, err)
	assert.NotEmpty(t, index)
	assert.Len(t, index[model.PageTypeOther], 5)
}

func TestClassifyDirect_ErrorFallsToOther(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Welcome"},
	}

	items := []anthropic.BatchRequestItem{
		{
			CustomID: "classify-0",
			Params: anthropic.MessageRequest{
				Model:     "claude-haiku-4-5-20251001",
				MaxTokens: 128,
				Messages:  []anthropic.Message{{Role: "user", Content: "test"}},
			},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(nil, errors.New("api error")).Once()

	usage := &model.TokenUsage{}
	index, _, err := classifyDirect(ctx, pages, items, aiClient, usage)

	assert.NoError(t, err)
	// Page should default to "other" type.
	assert.Len(t, index[model.PageTypeOther], 1)
	assert.Equal(t, "https://acme.com", index[model.PageTypeOther][0].URL)
}

func TestClassifyBatch_MissingResults(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com/page0", Markdown: "Content 0"},
		{URL: "https://acme.com/page1", Markdown: "Content 1"},
		{URL: "https://acme.com/page2", Markdown: "Content 2"},
		{URL: "https://acme.com/page3", Markdown: "Content 3"},
	}

	items := make([]anthropic.BatchRequestItem, 4)
	for i := range items {
		items[i] = anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("classify-%d", i),
			Params:   anthropic.MessageRequest{Model: "haiku", MaxTokens: 128, Messages: []anthropic.Message{{Role: "user", Content: "test"}}},
		}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{ID: "b1", ProcessingStatus: "ended"}, nil)

	aiClient.On("GetBatch", mock.Anything, "b1").
		Return(&anthropic.BatchResponse{ID: "b1", ProcessingStatus: "ended"}, nil)

	// Only return results for pages 0 and 2 (skip 1 and 3).
	resultItems := []anthropic.BatchResultItem{
		{
			CustomID: "classify-0",
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 50, OutputTokens: 10},
			},
		},
		{
			CustomID: "classify-2",
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"page_type": "services", "confidence": 0.85}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 50, OutputTokens: 10},
			},
		},
	}

	aiClient.On("GetBatchResults", mock.Anything, "b1").
		Return(setupBatchIterator(t, resultItems), nil)

	usage := &model.TokenUsage{}
	index, _, err := classifyBatch(ctx, pages, items, aiClient, usage)

	assert.NoError(t, err)
	// Pages 1 and 3 should default to "other".
	assert.Len(t, index[model.PageTypeOther], 2)
	assert.Len(t, index[model.PageTypeAbout], 1)
	assert.Len(t, index[model.PageTypeServices], 1)
}

func TestClassifyBatch_PollError(t *testing.T) {
	ctx := context.Background()

	pages := make([]model.CrawledPage, 4)
	items := make([]anthropic.BatchRequestItem, 4)
	for i := range items {
		pages[i] = model.CrawledPage{URL: fmt.Sprintf("https://acme.com/%d", i), Markdown: "x"}
		items[i] = anthropic.BatchRequestItem{CustomID: fmt.Sprintf("classify-%d", i)}
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{ID: "b1", ProcessingStatus: "in_progress"}, nil)

	aiClient.On("GetBatch", mock.Anything, "b1").
		Return(nil, errors.New("poll failed"))

	usage := &model.TokenUsage{}
	index, _, err := classifyBatch(ctx, pages, items, aiClient, usage)

	assert.Nil(t, index)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "classify: poll batch")
}

func TestClassifyBatch_GetResultsError(t *testing.T) {
	ctx := context.Background()

	pages := make([]model.CrawledPage, 4)
	items := make([]anthropic.BatchRequestItem, 4)
	for i := range items {
		pages[i] = model.CrawledPage{URL: fmt.Sprintf("https://acme.com/%d", i), Markdown: "x"}
		items[i] = anthropic.BatchRequestItem{CustomID: fmt.Sprintf("classify-%d", i)}
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{ID: "b1", ProcessingStatus: "ended"}, nil)

	aiClient.On("GetBatch", mock.Anything, "b1").
		Return(&anthropic.BatchResponse{ID: "b1", ProcessingStatus: "ended"}, nil)

	aiClient.On("GetBatchResults", mock.Anything, "b1").
		Return(nil, errors.New("stream error"))

	usage := &model.TokenUsage{}
	index, _, err := classifyBatch(ctx, pages, items, aiClient, usage)

	assert.Nil(t, index)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "classify: get batch results")
}
