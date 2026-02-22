package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
	"github.com/sells-group/research-cli/pkg/ppp"
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

	result, err := ExtractTier1(ctx, routed, nil, aiClient, aiCfg)

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

	result, err := ExtractTier1(ctx, nil, nil, aiClient, aiCfg)
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

	result, err := ExtractTier2(ctx, routed, t1Answers, nil, aiClient, aiCfg)

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

	result, err := ExtractTier3(ctx, routed, allAnswers, pages, nil, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Len(t, result.Answers, 1)
	assert.Equal(t, "AI expansion", result.Answers[0].Value)
	aiClient.AssertExpectations(t)
}

func TestExtractTier1_RichPrompt_MultiField(t *testing.T) {
	ctx := context.Background()

	// Multi-field question with Instructions triggers the rich prompt template.
	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q-grouped",
				Text:         "Company basics",
				FieldKey:     "company_name, year_established, owner_name",
				Instructions: "Extract the company name, year established, and owner name.",
				OutputFormat: `{"company_name": "string", "year_established": "number", "owner_name": "string", "confidence": 0.0}`,
			},
			Pages: []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "Acme Corp was founded in 2010 by Jane Doe."}}},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Capture the request to verify rich template was used.
	var capturedReq anthropic.MessageRequest
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		capturedReq = req
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"company_name": "Acme Corp", "year_established": 2010, "owner_name": "Jane Doe", "confidence": 0.9}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 80},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, 1, result.Tier)
	// Multi-field → 3 answers split from one response.
	assert.Len(t, result.Answers, 3)

	// Verify the prompt used Instructions (not q.Text) as the primary content.
	assert.Contains(t, capturedReq.Messages[0].Content, "Extract the company name")
	assert.Contains(t, capturedReq.Messages[0].Content, "Output JSON schema:")

	// Verify the rich system prompt was used.
	require.NotEmpty(t, capturedReq.System)
	assert.Contains(t, capturedReq.System[0].Text, "Return valid JSON matching the requested schema")

	// Verify MaxTokens is at least the minimum for multi-field.
	assert.GreaterOrEqual(t, capturedReq.MaxTokens, int64(512))

	aiClient.AssertExpectations(t)
}

func TestExtractTier2_RichPrompt_MultiField(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q-grouped",
				Text:         "Company details",
				FieldKey:     "company_name, revenue",
				Instructions: "Synthesize company name and revenue from all sources.",
				OutputFormat: `{"company_name": "string", "revenue": "string", "confidence": 0.0}`,
			},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "Acme Corp, Revenue $10M"}},
			},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	var capturedReq anthropic.MessageRequest
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		capturedReq = req
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"company_name": "Acme Corp", "revenue": "$10M", "confidence": 0.85}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, nil, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Len(t, result.Answers, 2)

	// Verify rich prompt.
	assert.Contains(t, capturedReq.Messages[0].Content, "Synthesize company name and revenue")
	assert.Contains(t, capturedReq.Messages[0].Content, "Output JSON schema:")
	require.NotEmpty(t, capturedReq.System)
	assert.Contains(t, capturedReq.System[0].Text, "Return valid JSON matching the requested schema")

	aiClient.AssertExpectations(t)
}

func TestParseExtractionAnswer_ValidJSON(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "industry"}
	text := `{"value": "Technology", "confidence": 0.9, "reasoning": "stated", "source_url": "https://acme.com"}`

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 1)
	assert.Equal(t, "q1", answers[0].QuestionID)
	assert.Equal(t, "industry", answers[0].FieldKey)
	assert.Equal(t, "Technology", answers[0].Value)
	assert.Equal(t, 0.9, answers[0].Confidence)
	assert.Equal(t, 1, answers[0].Tier)
	assert.Equal(t, "https://acme.com", answers[0].SourceURL)
}

func TestParseExtractionAnswer_InvalidJSON(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "industry"}
	text := "This is not JSON at all"

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 1)
	assert.Equal(t, "q1", answers[0].QuestionID)
	assert.Nil(t, answers[0].Value, "malformed JSON should produce nil Value, not raw text")
	assert.Equal(t, 0.0, answers[0].Confidence)
}

func TestParseExtractionAnswer_JSONWithCodeFence(t *testing.T) {
	q := model.Question{ID: "q1", FieldKey: "employees"}
	text := "```json\n{\"value\": 150, \"confidence\": 0.85, \"reasoning\": \"from page\", \"source_url\": \"https://acme.com\"}\n```"

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 1)
	assert.Equal(t, float64(150), answers[0].Value)
	assert.Equal(t, 0.85, answers[0].Confidence)
}

func TestParseExtractionAnswer_MultiField(t *testing.T) {
	q := model.Question{
		ID:       "q-group1",
		FieldKey: "company_name, year_established, owner_name",
	}
	text := `{"company_name": "Acme Corp", "year_established": 2010, "owner_name": "Jane Doe", "confidence": 0.85, "reasoning": "found on about page"}`

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 3)

	// Verify each field got its own answer.
	byKey := make(map[string]model.ExtractionAnswer)
	for _, a := range answers {
		byKey[a.FieldKey] = a
	}

	assert.Equal(t, "Acme Corp", byKey["company_name"].Value)
	assert.Equal(t, float64(2010), byKey["year_established"].Value)
	assert.Equal(t, "Jane Doe", byKey["owner_name"].Value)

	// Global confidence applied to all.
	for _, a := range answers {
		assert.Equal(t, 0.85, a.Confidence)
		assert.Equal(t, "q-group1", a.QuestionID)
		assert.Equal(t, 1, a.Tier)
	}
}

func TestParseExtractionAnswer_MultiField_MissingFields(t *testing.T) {
	q := model.Question{
		ID:       "q-group2",
		FieldKey: "company_name, year_established, owner_name",
	}
	// Only company_name is present; year_established and owner_name are absent.
	text := `{"company_name": "Acme Corp", "confidence": 0.6}`

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 3) // All 3 fields: 1 found + 2 null.
	byKey := make(map[string]model.ExtractionAnswer)
	for _, a := range answers {
		byKey[a.FieldKey] = a
	}
	assert.Equal(t, "Acme Corp", byKey["company_name"].Value)
	assert.Equal(t, 0.6, byKey["company_name"].Confidence)
	assert.Nil(t, byKey["year_established"].Value)
	assert.Equal(t, 0.3, byKey["year_established"].Confidence) // Halved confidence.
	assert.Nil(t, byKey["owner_name"].Value)
	assert.Equal(t, 0.3, byKey["owner_name"].Confidence)
}

func TestParseExtractionAnswer_MultiField_NullValues(t *testing.T) {
	q := model.Question{
		ID:       "q-group3",
		FieldKey: "company_name, year_established",
	}
	text := `{"company_name": "Acme", "year_established": null, "confidence": 0.7}`

	answers := parseExtractionAnswer(text, q, 1)

	// null values are still included (the key exists in the JSON).
	require.Len(t, answers, 2)
	byKey := make(map[string]model.ExtractionAnswer)
	for _, a := range answers {
		byKey[a.FieldKey] = a
	}
	assert.Equal(t, "Acme", byKey["company_name"].Value)
	assert.Nil(t, byKey["year_established"].Value)
}

func TestParseExtractionAnswer_LegacySingleField(t *testing.T) {
	// Even with comma in FieldKey, if there's a "value" key and only 1 field, use legacy.
	q := model.Question{ID: "q1", FieldKey: "industry"}
	text := `{"value": "Technology", "confidence": 0.9, "reasoning": "stated", "source_url": "https://acme.com"}`

	answers := parseExtractionAnswer(text, q, 1)

	require.Len(t, answers, 1)
	assert.Equal(t, "Technology", answers[0].Value)
	assert.Equal(t, 0.9, answers[0].Confidence)
}

func TestParseExtractionAnswer_MultiField_NoMatchingKeys(t *testing.T) {
	q := model.Question{
		ID:       "q-nomatch",
		FieldKey: "x, y, z",
	}
	// JSON has keys a and b, but none of x, y, z.
	text := `{"a": 1, "b": 2, "confidence": 0.5, "reasoning": "test"}`

	answers := parseExtractionAnswer(text, q, 1)

	// All 3 field keys emit null answers (none found in JSON).
	require.Len(t, answers, 3)
	for _, a := range answers {
		assert.Nil(t, a.Value)
		assert.Equal(t, 0.25, a.Confidence) // 0.5 * 0.5 halved.
		assert.Equal(t, "q-nomatch", a.QuestionID)
	}
}

func TestParseExtractionAnswer_SingleField_NoValueKey(t *testing.T) {
	// Single-field question but JSON uses the field name as key, not "value".
	q := model.Question{ID: "q-novalue", FieldKey: "industry"}
	text := `{"industry": "Technology", "confidence": 0.8}`

	answers := parseExtractionAnswer(text, q, 1)

	// Should fall through to multi-field path and find "industry" key.
	require.Len(t, answers, 1)
	assert.Equal(t, "industry", answers[0].FieldKey)
	assert.Equal(t, "Technology", answers[0].Value)
	assert.Equal(t, 0.8, answers[0].Confidence)
}

func TestParseExtractionAnswer_MultiField_MetaKeysIgnored(t *testing.T) {
	// Verify that meta keys (confidence, reasoning, source_url) in JSON don't
	// become field values when they're not in the FieldKey list.
	q := model.Question{
		ID:       "q-meta",
		FieldKey: "company_name, revenue",
	}
	text := `{"company_name": "Acme", "revenue": 5000000, "confidence": 0.9, "reasoning": "found", "source_url": "https://acme.com", "flags": ["verified"]}`

	answers := parseExtractionAnswer(text, q, 1)

	// Should only return 2 answers (company_name, revenue), not 6.
	require.Len(t, answers, 2)
	byKey := make(map[string]model.ExtractionAnswer)
	for _, a := range answers {
		byKey[a.FieldKey] = a
	}
	assert.Equal(t, "Acme", byKey["company_name"].Value)
	assert.Equal(t, float64(5000000), byKey["revenue"].Value)
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want float64
		ok   bool
	}{
		{"float64", float64(3.14), 3.14, true},
		{"int", int(42), 42.0, true},
		{"int64", int64(100), 100.0, true},
		{"string", "3.14", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.val)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSplitFieldKeys(t *testing.T) {
	assert.Equal(t, []string{"industry"}, splitFieldKeys("industry"))
	assert.Equal(t, []string{"a", "b", "c"}, splitFieldKeys("a, b, c"))
	assert.Equal(t, []string{"a", "b"}, splitFieldKeys("a,b"))
	assert.Equal(t, []string{"solo"}, splitFieldKeys("  solo  "))
	assert.Empty(t, splitFieldKeys(""))
	assert.Empty(t, splitFieldKeys(", , ,"))
}

func TestMaxTokensForQuestion(t *testing.T) {
	// Single field → 512.
	assert.Equal(t, int64(512), maxTokensForQuestion(model.Question{FieldKey: "industry"}))
	// 3 fields → 512 (min).
	assert.Equal(t, int64(512), maxTokensForQuestion(model.Question{FieldKey: "a, b, c"}))
	// 10 fields → 1000.
	assert.Equal(t, int64(1000), maxTokensForQuestion(model.Question{FieldKey: "a,b,c,d,e,f,g,h,i,j"}))
	// 50 fields → capped at 4096.
	keys := strings.Repeat("f,", 50)
	keys = keys[:len(keys)-1] // remove trailing comma
	assert.Equal(t, int64(4096), maxTokensForQuestion(model.Question{FieldKey: keys}))
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

	result, err := ExtractTier1(ctx, routed, nil, aiClient, aiCfg)

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

	result, err := ExtractTier2(ctx, routed, nil, nil, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, result.Answers, 5)
	// No primer + no batch = exactly 5 CreateMessage calls.
	aiClient.AssertNumberOfCalls(t, "CreateMessage", 5)
	aiClient.AssertNotCalled(t, "CreateBatch", mock.Anything, mock.Anything)
}

// --- Cache control stripping ---

func TestExtractTier1_BatchItems_HaveCacheControl(t *testing.T) {
	// Verify that both primer and batch items use BuildCachedSystemBlocks
	// so batch items signal cache reads and benefit from the primer's warm cache.
	const systemText = "You are a research analyst."

	// Both primer and batch items should use BuildCachedSystemBlocks.
	blocks := anthropic.BuildCachedSystemBlocks(systemText)
	require.Len(t, blocks, 1)
	require.NotNil(t, blocks[0].CacheControl, "system blocks SHOULD have CacheControl")
	assert.Equal(t, "1h", blocks[0].CacheControl.TTL)
	assert.Equal(t, systemText, blocks[0].Text)
}

func TestExtractTier1_AllRequestsUseCachedBlocks(t *testing.T) {
	ctx := context.Background()

	// Need 2+ items and NoBatch=false to trigger primer path.
	routed := makeRoutedQuestions(3)
	for i := range routed {
		routed[i].Pages = []model.ClassifiedPage{{
			CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "content"},
		}}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// All requests (primer + direct) should have CacheControl on system blocks.
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		return len(req.System) > 0 && req.System[0].CacheControl != nil
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "answer", "confidence": 0.9, "reasoning": "ok", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
	}, nil).Times(4) // 1 primer + 3 direct calls

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}
	result, err := ExtractTier1(ctx, routed, nil, aiClient, aiCfg)

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

// --- T2 confidence filtering ---

func TestExtractTier2_FiltersHighConfidenceT1(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Synthesize revenue", FieldKey: "revenue", OutputFormat: "string"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "Revenue $10M"}},
			},
		},
	}

	// 2 high-confidence (0.9), 2 low-confidence (0.3).
	// Only the low-confidence answers should appear in the T2 prompt context.
	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q10", FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		{QuestionID: "q11", FieldKey: "founded", Value: "2010", Confidence: 0.9, Tier: 1},
		{QuestionID: "q12", FieldKey: "revenue", Value: "unknown", Confidence: 0.3, Tier: 1},
		{QuestionID: "q13", FieldKey: "employees", Value: "~50", Confidence: 0.3, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Capture the prompt to verify which T1 answers are included.
	var capturedPrompt string
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		// Any MessageRequest qualifies; we just capture the prompt.
		if len(req.Messages) > 0 {
			capturedPrompt = req.Messages[0].Content
		}
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "$10M", "confidence": 0.9, "reasoning": "found", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Len(t, result.Answers, 1)

	// Low-confidence answers (revenue, employees) should be present in the prompt.
	assert.Contains(t, capturedPrompt, "revenue")
	assert.Contains(t, capturedPrompt, "unknown")
	assert.Contains(t, capturedPrompt, "0.30")
	assert.Contains(t, capturedPrompt, "employees")
	assert.Contains(t, capturedPrompt, "~50")

	// High-confidence answers (industry, founded) should NOT be in the prompt.
	assert.NotContains(t, capturedPrompt, "industry: Tech")
	assert.NotContains(t, capturedPrompt, "founded: 2010")
}

func TestExtractTier2_AllHighConfidence_EmptyContext(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Revenue?", FieldKey: "revenue", OutputFormat: "string"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "Revenue $10M"}},
			},
		},
	}

	// All T1 answers above 0.4 threshold — none should pass the filter.
	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q10", FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		{QuestionID: "q11", FieldKey: "founded", Value: "2010", Confidence: 0.85, Tier: 1},
		{QuestionID: "q12", FieldKey: "size", Value: "200", Confidence: 0.5, Tier: 1},
		{QuestionID: "q13", FieldKey: "location", Value: "NYC", Confidence: 0.4, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	var capturedPrompt string
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		if len(req.Messages) > 0 {
			capturedPrompt = req.Messages[0].Content
		}
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "$10M", "confidence": 0.92, "reasoning": "found", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Len(t, result.Answers, 1)

	// With all high-confidence answers filtered out, buildT1Context receives
	// an empty slice and returns "No previous findings."
	assert.Contains(t, capturedPrompt, "No previous findings.")
}

func TestExtractTier2_AllLowConfidence_FullContext(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Revenue?", FieldKey: "revenue", OutputFormat: "string"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com", Markdown: "Revenue $10M"}},
			},
		},
	}

	// All T1 answers below 0.4 threshold — all should be included in context.
	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q10", FieldKey: "industry", Value: "Tech", Confidence: 0.2, Tier: 1},
		{QuestionID: "q11", FieldKey: "founded", Value: "2010", Confidence: 0.1, Tier: 1},
		{QuestionID: "q12", FieldKey: "size", Value: "maybe 200", Confidence: 0.35, Tier: 1},
		{QuestionID: "q13", FieldKey: "location", Value: "possibly NYC", Confidence: 0.39, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	var capturedPrompt string
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		if len(req.Messages) > 0 {
			capturedPrompt = req.Messages[0].Content
		}
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": "$10M", "confidence": 0.88, "reasoning": "synthesized", "source_url": "https://acme.com"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Len(t, result.Answers, 1)

	// All four low-confidence answers should appear in the prompt.
	assert.Contains(t, capturedPrompt, "industry: Tech")
	assert.Contains(t, capturedPrompt, "0.20")
	assert.Contains(t, capturedPrompt, "founded: 2010")
	assert.Contains(t, capturedPrompt, "0.10")
	assert.Contains(t, capturedPrompt, "size: maybe 200")
	assert.Contains(t, capturedPrompt, "0.35")
	assert.Contains(t, capturedPrompt, "location: possibly NYC")
	assert.Contains(t, capturedPrompt, "0.39")

	// Should NOT contain the "No previous findings." placeholder.
	assert.NotContains(t, capturedPrompt, "No previous findings.")
}

func TestBuildT1Context_FilteredInput(t *testing.T) {
	tests := []struct {
		name     string
		answers  []model.ExtractionAnswer
		contains []string
		excludes []string
	}{
		{
			name:     "empty slice returns placeholder",
			answers:  nil,
			contains: []string{"No previous findings."},
		},
		{
			name: "single answer formatted correctly",
			answers: []model.ExtractionAnswer{
				{FieldKey: "revenue", Value: "unknown", Confidence: 0.3},
			},
			contains: []string{"- revenue: unknown (confidence: 0.30)"},
			excludes: []string{"No previous findings."},
		},
		{
			name: "multiple answers all included with correct format",
			answers: []model.ExtractionAnswer{
				{FieldKey: "industry", Value: "Tech", Confidence: 0.2},
				{FieldKey: "size", Value: "~50", Confidence: 0.35},
				{FieldKey: "location", Value: "NYC", Confidence: 0.1},
			},
			contains: []string{
				"- industry: Tech (confidence: 0.20)",
				"- size: ~50 (confidence: 0.35)",
				"- location: NYC (confidence: 0.10)",
			},
			excludes: []string{"No previous findings."},
		},
		{
			name: "numeric value formatted correctly",
			answers: []model.ExtractionAnswer{
				{FieldKey: "employees", Value: float64(150), Confidence: 0.25},
			},
			contains: []string{"- employees: 150 (confidence: 0.25)"},
		},
		{
			name: "answers joined by newlines",
			answers: []model.ExtractionAnswer{
				{FieldKey: "a", Value: "val1", Confidence: 0.1},
				{FieldKey: "b", Value: "val2", Confidence: 0.2},
			},
			contains: []string{"\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildT1Context(tt.answers)

			for _, s := range tt.contains {
				assert.Contains(t, result, s)
			}
			for _, s := range tt.excludes {
				assert.NotContains(t, result, s)
			}
		})
	}
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

// --- FormatPPPContext tests ---

func TestFormatPPPContext(t *testing.T) {
	loanDate := time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC)
	matches := []ppp.LoanMatch{
		{
			BorrowerName:    "ACME CORP LLC",
			CurrentApproval: 500_000,
			JobsReported:    25,
			NAICSCode:       "541511",
			BusinessType:    "LLC",
			BusinessAge:     "Existing or more than 2 years old",
			DateApproved:    loanDate,
			LoanStatus:      "Paid in Full",
			MatchScore:      1.0,
		},
	}

	result := FormatPPPContext(matches)

	assert.Contains(t, result, "--- PPP Loan Record (Federal Database) ---")
	assert.Contains(t, result, "Borrower: ACME CORP LLC")
	assert.Contains(t, result, "Loan Amount: $500000")
	assert.Contains(t, result, "Jobs Reported: 25")
	assert.Contains(t, result, "NAICS: 541511")
	assert.Contains(t, result, "Business Type: LLC")
	assert.Contains(t, result, "Business Age: Existing or more than 2 years old")
	assert.Contains(t, result, "Approved: 2020-06-15")
	assert.Contains(t, result, "Status: Paid in Full")
}

func TestFormatPPPContext_Empty(t *testing.T) {
	assert.Equal(t, "", FormatPPPContext(nil))
	assert.Equal(t, "", FormatPPPContext([]ppp.LoanMatch{}))
}

func TestFormatPPPContext_PartialData(t *testing.T) {
	matches := []ppp.LoanMatch{
		{
			BorrowerName:    "PARTIAL CORP",
			CurrentApproval: 100_000,
			// JobsReported: 0 → omitted
			// NAICSCode: "" → omitted
			BusinessType: "Corporation",
			MatchScore:   0.8,
		},
	}

	result := FormatPPPContext(matches)

	assert.Contains(t, result, "Borrower: PARTIAL CORP")
	assert.Contains(t, result, "Loan Amount: $100000")
	assert.Contains(t, result, "Business Type: Corporation")
	assert.NotContains(t, result, "Jobs Reported")
	assert.NotContains(t, result, "NAICS")
	assert.NotContains(t, result, "Business Age")
}
