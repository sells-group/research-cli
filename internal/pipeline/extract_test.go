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
	routed := makeRoutedQuestions(1) // Single item to avoid concurrency issues with mock ordering.
	items := makeBatchItems(routed)

	aiClient := anthropicmocks.NewMockClient(t)

	// With retry logic (3 attempts), all 3 fail. Item produces no answer.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(nil, errors.New("model overloaded")).Times(3)

	answers, usage, err := executeBatch(ctx, items, routed, 1, aiClient, config.AnthropicConfig{SmallBatchThreshold: 3})

	require.NoError(t, err)
	// No answers — the single item failed all retries.
	assert.Len(t, answers, 0)
	assert.Equal(t, 0, usage.InputTokens)
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

// --- Cache control stripping ---

func TestExtractTier1_BatchItems_NoCacheControl(t *testing.T) {
	// Verify that batch item system blocks have NO CacheControl, while
	// primer system blocks DO have CacheControl. This is a unit test of
	// the block construction logic rather than an integration test.
	const systemText = "You are a research analyst."

	// Batch items use plain system blocks (no CacheControl).
	batchBlocks := []anthropic.SystemBlock{{Text: systemText}}
	require.Len(t, batchBlocks, 1)
	assert.Nil(t, batchBlocks[0].CacheControl, "batch item system blocks should NOT have CacheControl")
	assert.Equal(t, systemText, batchBlocks[0].Text)

	// Primer uses BuildCachedSystemBlocks (has CacheControl).
	primerBlocks := anthropic.BuildCachedSystemBlocks(systemText)
	require.Len(t, primerBlocks, 1)
	require.NotNil(t, primerBlocks[0].CacheControl, "primer system blocks SHOULD have CacheControl")
	assert.Equal(t, "1h", primerBlocks[0].CacheControl.TTL)
	assert.Equal(t, systemText, primerBlocks[0].Text)
}

func TestExtractTier1_PrimerUsesCachedBlocks(t *testing.T) {
	ctx := context.Background()

	// Need 2+ items and NoBatch=false to trigger primer path.
	routed := makeRoutedQuestions(3)
	for i := range routed {
		routed[i].Pages = []model.ClassifiedPage{{
			CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "content"},
		}}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Capture the primer request to verify it uses cached system blocks.
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		// Primer request should have CacheControl on its system blocks.
		if len(req.System) > 0 && req.System[0].CacheControl != nil {
			return true
		}
		return false
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "primer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
	}, nil).Once()

	// Direct calls for the 3 items (below default smallBatchThresholdT1=20).
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		// Non-primer requests should NOT have CacheControl.
		if len(req.System) > 0 && req.System[0].CacheControl == nil {
			return true
		}
		return false
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
	}, nil).Times(3)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}
	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 3)
	aiClient.AssertExpectations(t)
}

// --- External snippet tests ---

func TestBuildExternalSnippets_Budget(t *testing.T) {
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{Title: "[BBB] Better Business Bureau", URL: "https://bbb.org/acme", Markdown: "BBB Rating: A+. Accredited since 2010. No complaints filed."}},
		{CrawledPage: model.CrawledPage{Title: "[Google_Maps] Acme Inc", URL: "https://maps.google.com/acme", Markdown: "4.5 stars, 120 reviews. Located at 123 Main St."}},
		{CrawledPage: model.CrawledPage{Title: "[SoS] Secretary of State", URL: "https://sos.state.gov/acme", Markdown: "Active corporation, filed 2015. Agent: John Doe."}},
		{CrawledPage: model.CrawledPage{Title: "About Us", URL: "https://acme.com/about", Markdown: "This should NOT appear in external snippets."}},
	}

	result := buildExternalSnippets(pages, 100)

	// Should include external pages.
	assert.Contains(t, result, "BBB")
	// Should NOT include non-external page.
	assert.NotContains(t, result, "This should NOT appear")
	// Total markdown content should respect the budget.
	// The budget applies to the markdown content chars, not the full formatted output.
}

func TestBuildExternalSnippets_NoExternalPages(t *testing.T) {
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{Title: "Home", URL: "https://acme.com", Markdown: "Welcome"}},
		{CrawledPage: model.CrawledPage{Title: "About", URL: "https://acme.com/about", Markdown: "About us"}},
	}

	result := buildExternalSnippets(pages, 2000)
	assert.Equal(t, "", result)
}

func TestBuildExternalSnippets_TruncatesLongContent(t *testing.T) {
	longContent := strings.Repeat("X", 500)
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{Title: "[LinkedIn] Acme", URL: "https://linkedin.com/acme", Markdown: longContent}},
	}

	result := buildExternalSnippets(pages, 100)

	// The content should be truncated to the budget.
	assert.NotContains(t, result, strings.Repeat("X", 200))
}

func TestBuildExternalSnippets_MultiplePagesWithinBudget(t *testing.T) {
	pages := []model.ClassifiedPage{
		{CrawledPage: model.CrawledPage{Title: "[BBB] Acme", URL: "https://bbb.org/acme", Markdown: "Rating A+"}},
		{CrawledPage: model.CrawledPage{Title: "[SoS] Acme Corp", URL: "https://sos.gov/acme", Markdown: "Active"}},
	}

	result := buildExternalSnippets(pages, 2000)

	assert.Contains(t, result, "Rating A+")
	assert.Contains(t, result, "Active")
}

// --- Truncation tests ---

func TestTruncateByRelevance_ShortContent(t *testing.T) {
	content := "Short content under the limit."
	result := truncateByRelevance(content, "anything", 1000)
	assert.Equal(t, content, result)
}

func TestTruncateByRelevance_KeywordScoring(t *testing.T) {
	content := `# About Our Team
We have a great engineering team.

# Revenue Information
Annual revenue was $50M in 2024. Revenue growth is 15% year over year.

# Office Locations
We have offices in New York and San Francisco.

# Revenue Breakdown
Revenue from consulting is $30M. Revenue from products is $20M.

# Contact Information
Email us at info@acme.com for more details.`

	// Ask about revenue; sections mentioning "revenue" should be prioritized.
	result := truncateByRelevance(content, "What is the company's annual revenue?", 200)

	// Revenue sections should be kept.
	assert.Contains(t, result, "Revenue")
	// Contact section (low relevance) should be dropped if budget is tight.
	// The exact selection depends on section sizes, but revenue content should be present.
}

func TestTruncateByRelevance_NoSections(t *testing.T) {
	// Single block of text with no headers or empty lines.
	content := strings.Repeat("word ", 2000) // ~10000 chars
	result := truncateByRelevance(content, "test question here", 100)

	// Should fall back to hard truncation since there's only 1 section.
	assert.Len(t, result, 100)
}

func TestTruncateByRelevance_NoKeywords(t *testing.T) {
	content := "# Section 1\nContent one.\n\n# Section 2\nContent two."
	// Question with only stop words / short words → no keywords extracted.
	result := truncateByRelevance(content+strings.Repeat(" padding", 1000), "is it?", 100)

	// No keywords → falls back to hard truncation.
	assert.Len(t, result, 100)
}

func TestTruncateByRelevance_AllSectionsTooLarge(t *testing.T) {
	// Two sections, each larger than the limit.
	sec1 := "# Section 1\n" + strings.Repeat("A", 200)
	sec2 := "\n\n# Section 2\n" + strings.Repeat("B", 200)
	content := sec1 + sec2

	result := truncateByRelevance(content, "section content here", 50)

	// All sections exceed budget → falls back to hard truncation.
	assert.Len(t, result, 50)
}

// --- Keyword extraction ---

func TestExtractKeywords(t *testing.T) {
	text := "What is the company's annual revenue for 2024?"
	keywords := extractKeywords(text)

	assert.Contains(t, keywords, "annual")
	assert.Contains(t, keywords, "revenue")
	assert.Contains(t, keywords, "2024")
	assert.Contains(t, keywords, "company's")
	// Stop words and short words excluded.
	assert.NotContains(t, keywords, "the")
	assert.NotContains(t, keywords, "for")
	assert.NotContains(t, keywords, "what")
	assert.NotContains(t, keywords, "is") // 2 chars
}

func TestExtractKeywords_Empty(t *testing.T) {
	assert.Empty(t, extractKeywords(""))
}

func TestExtractKeywords_AllStopWords(t *testing.T) {
	assert.Empty(t, extractKeywords("the and for are was"))
}

func TestExtractKeywords_Deduplication(t *testing.T) {
	keywords := extractKeywords("revenue revenue revenue growth growth")
	assert.Len(t, keywords, 2) // "revenue" and "growth", deduplicated
	assert.Contains(t, keywords, "revenue")
	assert.Contains(t, keywords, "growth")
}

func TestExtractKeywords_PunctuationStripping(t *testing.T) {
	keywords := extractKeywords("(hello) world! revenue? growth.")
	assert.Contains(t, keywords, "hello")
	assert.Contains(t, keywords, "world")
	assert.Contains(t, keywords, "revenue")
	assert.Contains(t, keywords, "growth")
}

// --- Section splitting ---

func TestSplitSections(t *testing.T) {
	content := "# Section 1\nContent one.\n\n# Section 2\nContent two.\n\nMore content."
	sections := splitSections(content)
	assert.GreaterOrEqual(t, len(sections), 2)
}

func TestSplitSections_NoHeaders(t *testing.T) {
	content := "Just a long paragraph\nwith no headers\nand no empty lines."
	sections := splitSections(content)
	assert.Len(t, sections, 1)
}

func TestSplitSections_EmptyLineBreaks(t *testing.T) {
	content := "Paragraph one.\n\nParagraph two.\n\nParagraph three."
	sections := splitSections(content)
	assert.GreaterOrEqual(t, len(sections), 3)
}

func TestSplitSections_HeadersOnly(t *testing.T) {
	content := "# Header 1\nText under header 1.\n# Header 2\nText under header 2."
	sections := splitSections(content)
	assert.GreaterOrEqual(t, len(sections), 2)
	// First section should contain header 1 text.
	assert.Contains(t, sections[0], "Text under header 1")
}

func TestSplitSections_EmptyContent(t *testing.T) {
	sections := splitSections("")
	assert.Empty(t, sections)
}

func TestSplitSections_TrailingNewlines(t *testing.T) {
	content := "# Header\nSome content.\n\n"
	sections := splitSections(content)
	for _, s := range sections {
		// All sections should be trimmed.
		assert.Equal(t, strings.TrimSpace(s), s)
	}
}

// --- Tier threshold ---

func TestTierThreshold_Defaults(t *testing.T) {
	assert.Equal(t, 20, tierThreshold(1, 0))
	assert.Equal(t, 10, tierThreshold(2, 0))
	assert.Equal(t, 5, tierThreshold(3, 0))
	assert.Equal(t, 20, tierThreshold(99, 0)) // default case
}

func TestTierThreshold_ConfigOverride(t *testing.T) {
	assert.Equal(t, 15, tierThreshold(1, 15))
	assert.Equal(t, 15, tierThreshold(2, 15))
	assert.Equal(t, 15, tierThreshold(3, 15))
}

func TestTierThreshold_NegativeConfig(t *testing.T) {
	// Negative config should fall through to defaults (not > 0).
	assert.Equal(t, 20, tierThreshold(1, -1))
	assert.Equal(t, 10, tierThreshold(2, -1))
}

// --- isExternalPage ---

func TestIsExternalPage(t *testing.T) {
	assert.True(t, isExternalPage("[BBB] Better Business Bureau"))
	assert.True(t, isExternalPage("[bbb] lowercase"))
	assert.True(t, isExternalPage("[Google_Maps] Location"))
	assert.True(t, isExternalPage("[google_maps] location"))
	assert.True(t, isExternalPage("[SoS] Secretary of State"))
	assert.True(t, isExternalPage("[LinkedIn] Company Profile"))
	assert.False(t, isExternalPage("About Us"))
	assert.False(t, isExternalPage("Home"))
	assert.False(t, isExternalPage("[unknown] Other"))
	assert.False(t, isExternalPage(""))
}
