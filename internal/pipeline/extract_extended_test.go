package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

func TestExtractTier1_WithInstructions(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q1",
				Text:         "What industry?",
				FieldKey:     "industry",
				OutputFormat: "string",
				Instructions: "Look for NAICS codes",
			},
			Pages: []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "We operate in technology (NAICS 541511)."}}},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "Technology (541511)", "confidence": 0.95, "reasoning": "NAICS code found", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 1)
	assert.Equal(t, "Technology (541511)", result.Answers[0].Value)
}

func TestExtractTier1_LongContent(t *testing.T) {
	ctx := context.Background()

	// Content longer than 8000 chars should be truncated.
	longContent := strings.Repeat("This is a very long page content. ", 500)

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "What?", FieldKey: "field", OutputFormat: "string"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: longContent}}},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 500, OutputTokens: 50},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 1)
}

func TestExtractTier2_EmptyRouted(t *testing.T) {
	ctx := context.Background()
	aiClient := anthropicmocks.NewMockClient(t)
	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, nil, nil, aiClient, aiCfg)
	assert.NoError(t, err)
	assert.Equal(t, 2, result.Tier)
	assert.Empty(t, result.Answers)
}

func TestExtractTier2_SingleQuestion(t *testing.T) {
	ctx := context.Background()

	// Single question: no primer (< 2 items), direct execution (<=3 items).
	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Revenue?", FieldKey: "revenue", OutputFormat: "string", Instructions: "Convert to USD"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "Revenue $10M"}},
			},
		},
	}

	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q0", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Only 1 direct call (no primer for single item).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "$10M", "confidence": 0.9, "reasoning": "found on page", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 2, result.Tier)
	assert.Len(t, result.Answers, 1)
}

func TestExtractTier3_EmptyRouted(t *testing.T) {
	ctx := context.Background()
	aiClient := anthropicmocks.NewMockClient(t)
	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001", OpusModel: "claude-opus-4-6"}

	result, err := ExtractTier3(ctx, nil, nil, nil, aiClient, aiCfg)
	assert.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Empty(t, result.Answers)
}

func TestExtractTier3_MultipleQuestions_BatchPath(t *testing.T) {
	ctx := context.Background()

	// 5 questions: triggers batch path (> 3) and primer (> 1).
	routed := make([]model.RoutedQuestion, 5)
	for i := 0; i < 5; i++ {
		routed[i] = model.RoutedQuestion{
			Question: model.Question{
				ID:           fmt.Sprintf("q%d", i),
				Text:         fmt.Sprintf("Complex question %d?", i),
				FieldKey:     fmt.Sprintf("field_%d", i),
				OutputFormat: "string",
			},
			Pages: []model.ClassifiedPage{{
				CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "content"},
			}},
		}
	}

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Test content for context preparation."},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech"},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// First call: Haiku summarization in prepareTier3Context.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Summary of company data."}},
			Usage:   anthropic.TokenUsage{InputTokens: 500, OutputTokens: 100},
		}, nil).Once()

	// Second call: T3 primer request (first of 5 items).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "primer", "confidence": 0.9}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 400, OutputTokens: 80, CacheCreationInputTokens: 200},
		}, nil).Once()

	// Batch API calls for the 5 items.
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-t3",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatch", mock.Anything, "batch-t3").
		Return(&anthropic.BatchResponse{
			ID:               "batch-t3",
			ProcessingStatus: "ended",
		}, nil)

	var resultItems []anthropic.BatchResultItem
	for i := 0; i < 5; i++ {
		resultItems = append(resultItems, anthropic.BatchResultItem{
			CustomID: fmt.Sprintf("t3-%d-q%d", i, i),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: fmt.Sprintf(`{"value": "t3_answer_%d", "confidence": 0.88, "reasoning": "expert analysis", "source_url": "https://acme.com"}`, i)}},
				Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 40},
			},
		})
	}

	aiClient.On("GetBatchResults", mock.Anything, "batch-t3").
		Return(setupBatchIterator(t, resultItems), nil)

	aiCfg := config.AnthropicConfig{
		HaikuModel: "claude-haiku-4-5-20251001",
		OpusModel:  "claude-opus-4-6",
	}

	result, err := ExtractTier3(ctx, routed, allAnswers, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Len(t, result.Answers, 5)
	aiClient.AssertExpectations(t)
}

func TestExtractTier3_ContextPreparationFails(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Strategic direction?", FieldKey: "strategy"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com"}}},
		},
	}

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "content"},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Summarization fails.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(nil, errors.New("api error")).Once()

	aiCfg := config.AnthropicConfig{
		HaikuModel: "claude-haiku-4-5-20251001",
		OpusModel:  "claude-opus-4-6",
	}

	result, err := ExtractTier3(ctx, routed, nil, pages, aiClient, aiCfg)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tier 3 context preparation")
}

func TestPrepareTier3Context_LongPages(t *testing.T) {
	ctx := context.Background()

	// Create pages with more than 3000 chars each and > 50000 total to trigger truncation.
	var pages []model.CrawledPage
	for i := 0; i < 20; i++ {
		pages = append(pages, model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/page%d", i),
			Title:    fmt.Sprintf("Page %d", i),
			Markdown: strings.Repeat("A", 4000),
		})
	}

	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech"},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Compact summary of all the data."}},
			Usage:   anthropic.TokenUsage{InputTokens: 1000, OutputTokens: 200},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	summary, usage, err := prepareTier3Context(ctx, pages, answers, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, "Compact summary of all the data.", summary)
	assert.Equal(t, 1000, usage.InputTokens)
	assert.Equal(t, 200, usage.OutputTokens)
}

func TestBuildPagesContext(t *testing.T) {
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme"}},
		{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Title: "About", Markdown: "About us page"}},
	}

	result := buildPagesContext(pages, 4000)
	assert.Contains(t, result, "Home")
	assert.Contains(t, result, "About")
	assert.Contains(t, result, "https://acme.com")
}

func TestBuildPagesContext_LongTruncation(t *testing.T) {
	longMarkdown := strings.Repeat("X", 5000)
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{URL: "https://acme.com", Title: "Home", Markdown: longMarkdown}},
	}

	result := buildPagesContext(pages, 100)
	// Should be truncated to 100 chars of content.
	assert.Contains(t, result, "Home")
	// The markdown content portion should be 100 chars.
	assert.Less(t, len(result), 5000)
}

func TestBuildPagesContext_Empty(t *testing.T) {
	result := buildPagesContext(nil, 4000)
	assert.Equal(t, "", result)
}

func TestExecuteBatch_Tier2_Prefix(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5)

	// Create batch items with t2 prefix.
	items := make([]anthropic.BatchRequestItem, len(routed))
	for i, rq := range routed {
		items[i] = anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t2-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     "claude-sonnet-4-5-20250929",
				MaxTokens: 1024,
				Messages:  []anthropic.Message{{Role: "user", Content: "test"}},
			},
		}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-t2",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatch", mock.Anything, "batch-t2").
		Return(&anthropic.BatchResponse{
			ID:               "batch-t2",
			ProcessingStatus: "ended",
		}, nil)

	var resultItems []anthropic.BatchResultItem
	for i, rq := range routed {
		resultItems = append(resultItems, anthropic.BatchResultItem{
			CustomID: fmt.Sprintf("t2-%d-%s", i, rq.Question.ID),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
			},
		})
	}

	aiClient.On("GetBatchResults", mock.Anything, "batch-t2").
		Return(setupBatchIterator(t, resultItems), nil)

	answers, usage, err := executeBatch(ctx, items, routed, 2, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	assert.Len(t, answers, 5)
	assert.Equal(t, 500, usage.InputTokens)
}

func TestExecuteBatch_Tier3_Prefix(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(4)

	items := make([]anthropic.BatchRequestItem, len(routed))
	for i, rq := range routed {
		items[i] = anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t3-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     "claude-opus-4-6",
				MaxTokens: 2048,
				Messages:  []anthropic.Message{{Role: "user", Content: "test"}},
			},
		}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-t3",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatch", mock.Anything, "batch-t3").
		Return(&anthropic.BatchResponse{
			ID:               "batch-t3",
			ProcessingStatus: "ended",
		}, nil)

	var resultItems []anthropic.BatchResultItem
	for i, rq := range routed {
		resultItems = append(resultItems, anthropic.BatchResultItem{
			CustomID: fmt.Sprintf("t3-%d-%s", i, rq.Question.ID),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.88, "reasoning": "expert", "source_url": "https://acme.com"}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 40},
			},
		})
	}

	aiClient.On("GetBatchResults", mock.Anything, "batch-t3").
		Return(setupBatchIterator(t, resultItems), nil)

	answers, usage, err := executeBatch(ctx, items, routed, 3, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	assert.Len(t, answers, 4)
	assert.Equal(t, 800, usage.InputTokens)
}

func TestParseExtractionAnswer_NumericValue(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "count"}
	text := `{"value": 42, "confidence": 0.8, "reasoning": "counted", "source_url": "https://acme.com"}`

	answer := parseExtractionAnswer(text, q, 1)

	assert.Equal(t, float64(42), answer.Value)
	assert.Equal(t, 0.8, answer.Confidence)
	assert.Equal(t, "counted", answer.Reasoning)
}

func TestParseExtractionAnswer_BoolValue(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "active"}
	text := `{"value": true, "confidence": 0.95, "reasoning": "stated", "source_url": "https://acme.com"}`

	answer := parseExtractionAnswer(text, q, 2)

	assert.Equal(t, true, answer.Value)
	assert.Equal(t, 0.95, answer.Confidence)
	assert.Equal(t, 2, answer.Tier)
}

func TestParseExtractionAnswer_NullValue(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "field"}
	text := `{"value": null, "confidence": 0.1, "reasoning": "not found", "source_url": ""}`

	answer := parseExtractionAnswer(text, q, 1)

	assert.Nil(t, answer.Value)
	assert.Equal(t, 0.1, answer.Confidence)
}
