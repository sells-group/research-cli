package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

func TestExtractTier1_DirectMode(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "What industry?", FieldKey: "industry", OutputFormat: "string"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "Acme is a technology company."}}},
		},
		{
			Question: model.Question{ID: "q2", Text: "How many employees?", FieldKey: "employees", OutputFormat: "number"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "We have 200 employees."}}},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Primer request (NoBatch=false, 2 items > 1 → primer fires) + 2 direct calls = 3 total.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "Technology", "confidence": 0.9, "reasoning": "stated on page", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Times(3)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 1, result.Tier)
	assert.Len(t, result.Answers, 2)
	// Primer(200) + 2 direct(200 each) = 600 input tokens total.
	assert.Equal(t, 600, result.TokenUsage.InputTokens)
	aiClient.AssertExpectations(t)
}

func TestExtractTier1_EmptyRouted(t *testing.T) {
	ctx := context.Background()
	aiClient := anthropicmocks.NewMockClient(t)
	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, nil, aiClient, aiCfg)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Tier)
	assert.Empty(t, result.Answers)
}

func TestExtractTier2_WithPrimer(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Synthesize revenue", FieldKey: "revenue"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "Revenue was $10M in 2024"}},
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/investors", Markdown: "Annual revenue: $10.5M"}},
			},
		},
		{
			Question: model.Question{ID: "q2", Text: "Company size", FieldKey: "size"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "200 employees"}},
			},
		},
	}

	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q0", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Primer request (first of 2+ items) + 2 direct calls (<=3 items).
	// The primer sends batchItems[0].Params, then executeBatch sends each item directly.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "$10.5M", "confidence": 0.92, "reasoning": "from investor page", "source_url": "https://acme.com/investors"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
		}, nil).Times(3) // primer + 2 direct calls

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 2, result.Tier)
	assert.Len(t, result.Answers, 2)
	aiClient.AssertExpectations(t)
}

func TestExtractTier3_WithSummarization(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Strategic direction?", FieldKey: "strategy"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com"}}},
		},
	}

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Acme is expanding into AI."},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech"},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Summarization (Haiku) call.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Acme Corp is a tech company expanding into AI services."}},
			Usage:   anthropic.TokenUsage{InputTokens: 500, OutputTokens: 100},
		}, nil).Once()

	// T3 extraction (direct mode, 1 item).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "AI expansion", "confidence": 0.88, "reasoning": "multiple sources", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 400, OutputTokens: 80},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{
		HaikuModel: "claude-haiku-4-5-20251001",
		OpusModel:  "claude-opus-4-6",
	}

	result, err := ExtractTier3(ctx, routed, allAnswers, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Len(t, result.Answers, 1)
	assert.Equal(t, "AI expansion", result.Answers[0].Value)
	aiClient.AssertExpectations(t)
}

func TestParseExtractionAnswer_ValidJSON(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "industry"}
	text := `{"value": "Technology", "confidence": 0.9, "reasoning": "stated", "source_url": "https://acme.com"}`

	answer := parseExtractionAnswer(text, q, 1)

	assert.Equal(t, "q1", answer.QuestionID)
	assert.Equal(t, "industry", answer.FieldKey)
	assert.Equal(t, "Technology", answer.Value)
	assert.Equal(t, 0.9, answer.Confidence)
	assert.Equal(t, 1, answer.Tier)
	assert.Equal(t, "https://acme.com", answer.SourceURL)
}

func TestParseExtractionAnswer_InvalidJSON(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "industry"}
	text := "This is not JSON at all"

	answer := parseExtractionAnswer(text, q, 1)

	assert.Equal(t, "q1", answer.QuestionID)
	assert.Equal(t, text, answer.Value)
	assert.Equal(t, 0.0, answer.Confidence)
}

func TestParseExtractionAnswer_JSONWithCodeFence(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "employees"}
	text := "```json\n{\"value\": 150, \"confidence\": 0.85, \"reasoning\": \"from page\", \"source_url\": \"https://acme.com\"}\n```"

	answer := parseExtractionAnswer(text, q, 1)

	assert.Equal(t, float64(150), answer.Value)
	assert.Equal(t, 0.85, answer.Confidence)
}

func TestExtractText(t *testing.T) {
	resp := &anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{
			{Text: "Hello"},
			{Text: " World"},
		},
	}
	assert.Equal(t, "Hello\n World", extractText(resp))
}

func TestExtractText_Nil(t *testing.T) {
	assert.Equal(t, "", extractText(nil))
}

func TestCleanJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain json", `{"key": "value"}`, `{"key": "value"}`},
		{"code fence", "```json\n{\"key\": \"value\"}\n```", `{"key": "value"}`},
		{"with prefix", "Here's the result: {\"key\": \"value\"}", `{"key": "value"}`},
		{"with suffix", "{\"key\": \"value\"} that's the answer", `{"key": "value"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanJSON(tt.input)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuildT1Context_Empty(t *testing.T) {
	result := buildT1Context(nil)
	assert.Equal(t, "No previous findings.", result)
}

func TestBuildT1Context_WithAnswers(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech", Confidence: 0.9},
	}
	result := buildT1Context(answers)
	assert.Contains(t, result, "industry")
	assert.Contains(t, result, "Tech")
	assert.Contains(t, result, "0.90")
}

// --- executeBatch batch-path tests ---

func makeRoutedQuestions(n int) []model.RoutedQuestion {
	routed := make([]model.RoutedQuestion, n)
	for i := 0; i < n; i++ {
		routed[i] = model.RoutedQuestion{
			Question: model.Question{
				ID:           fmt.Sprintf("q%d", i),
				Text:         fmt.Sprintf("Question %d?", i),
				FieldKey:     fmt.Sprintf("field_%d", i),
				OutputFormat: "string",
			},
			Pages: []model.ClassifiedPage{{
				CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "content"},
			}},
		}
	}
	return routed
}

func makeBatchItems(routed []model.RoutedQuestion) []anthropic.BatchRequestItem {
	items := make([]anthropic.BatchRequestItem, len(routed))
	for i, rq := range routed {
		items[i] = anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t1-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     "claude-haiku-4-5-20251001",
				MaxTokens: 512,
				Messages: []anthropic.Message{
					{Role: "user", Content: "test prompt"},
				},
			},
		}
	}
	return items
}

func TestExecuteBatch_BatchPath(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5)
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)

	// CreateBatch returns a batch ID.
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	// PollBatch wraps ctx with a timeout, so use mock.Anything for context.
	aiClient.On("GetBatch", mock.Anything, "batch-1").
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	// Build 5 batch results.
	var resultItems []anthropic.BatchResultItem
	for i, rq := range routed {
		resultItems = append(resultItems, anthropic.BatchResultItem{
			CustomID: fmt.Sprintf("t1-%d-%s", i, rq.Question.ID),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: fmt.Sprintf(`{"value": "answer_%d", "confidence": 0.9, "reasoning": "found", "source_url": "https://acme.com"}`, i)}},
				Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
			},
		})
	}

	aiClient.On("GetBatchResults", mock.Anything, "batch-1").
		Return(setupBatchIterator(t, resultItems), nil)

	answers, usage, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	assert.Len(t, answers, 5)
	assert.Equal(t, 500, usage.InputTokens)  // 5 * 100
	assert.Equal(t, 100, usage.OutputTokens) // 5 * 20
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_CreateBatchError(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5)
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(nil, errors.New("rate limited"))

	answers, _, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	assert.Nil(t, answers)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute batch: create")
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_PollError(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5)
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "in_progress",
		}, nil)

	// PollBatch wraps ctx with a timeout, so use mock.Anything for context.
	aiClient.On("GetBatch", mock.Anything, "batch-1").
		Return(nil, errors.New("api error"))

	answers, _, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	assert.Nil(t, answers)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute batch: poll")
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_GetResultsError(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5)
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatch", mock.Anything, "batch-1").
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatchResults", mock.Anything, "batch-1").
		Return(nil, errors.New("stream error"))

	answers, _, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	assert.Nil(t, answers)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute batch: get results")
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_MissingResultInBatch(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(4)
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	aiClient.On("GetBatch", mock.Anything, "batch-1").
		Return(&anthropic.BatchResponse{
			ID:               "batch-1",
			ProcessingStatus: "ended",
		}, nil)

	// Only return results for items 0 and 2 (skip 1 and 3).
	resultItems := []anthropic.BatchResultItem{
		{
			CustomID: fmt.Sprintf("t1-0-%s", routed[0].Question.ID),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"value": "a0", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
			},
		},
		{
			CustomID: fmt.Sprintf("t1-2-%s", routed[2].Question.ID),
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"value": "a2", "confidence": 0.8, "reasoning": "ok", "source_url": "https://acme.com"}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
			},
		},
	}

	aiClient.On("GetBatchResults", mock.Anything, "batch-1").
		Return(setupBatchIterator(t, resultItems), nil)

	answers, usage, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	assert.Len(t, answers, 2)
	assert.Equal(t, 200, usage.InputTokens)
	assert.Equal(t, 40, usage.OutputTokens)
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_DirectModeError(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(2) // <=3 triggers direct mode.
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)

	// First call fails.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(nil, errors.New("model overloaded")).Once()

	// Second call succeeds.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Once()

	answers, usage, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	// Only 1 answer (first failed, second succeeded).
	assert.Len(t, answers, 1)
	assert.Equal(t, "answer", answers[0].Value)
	assert.Equal(t, 100, usage.InputTokens)
	aiClient.AssertExpectations(t)
}

func TestExecuteBatch_NoBatchForcesDirectPath(t *testing.T) {
	ctx := context.Background()
	routed := makeRoutedQuestions(5) // >3 items would normally use batch
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)

	// With noBatch=true, all 5 items should use CreateMessage (direct), not CreateBatch.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "direct_answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Times(5)

	answers, usage, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{NoBatch: true, SmallBatchThreshold: 3})

	require.NoError(t, err)
	assert.Len(t, answers, 5)
	assert.Equal(t, 500, usage.InputTokens)
	assert.Equal(t, 100, usage.OutputTokens)
	// Verify CreateBatch was never called.
	aiClient.AssertNotCalled(t, "CreateBatch", mock.Anything, mock.Anything)
	aiClient.AssertExpectations(t)
}

func TestExtractTier1_NoBatch(t *testing.T) {
	ctx := context.Background()

	// 5 questions: would normally trigger batch, but NoBatch forces direct.
	routed := makeRoutedQuestions(5)
	for i := range routed {
		routed[i].Pages = []model.ClassifiedPage{{
			CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "Acme is a technology company."},
		}}
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Times(5)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001", NoBatch: true}

	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 5)
	aiClient.AssertNotCalled(t, "CreateBatch", mock.Anything, mock.Anything)
	aiClient.AssertExpectations(t)
}

func TestExtractTier2_NoBatch_SkipsPrimer(t *testing.T) {
	ctx := context.Background()

	routed := makeRoutedQuestions(5)
	for i := range routed {
		routed[i].Pages = []model.ClassifiedPage{{
			CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "content"},
		}}
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Only 5 direct calls (no primer).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Times(5)

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929", NoBatch: true}

	result, err := ExtractTier2(ctx, routed, nil, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 5)
	// No primer + no batch = exactly 5 CreateMessage calls.
	aiClient.AssertNumberOfCalls(t, "CreateMessage", 5)
	aiClient.AssertNotCalled(t, "CreateBatch", mock.Anything, mock.Anything)
}
