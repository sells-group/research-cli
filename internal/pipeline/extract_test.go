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

	aiClient := &mockAnthropicClient{}
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "Technology", "confidence": 0.9, "reasoning": "stated on page", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Once()

	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": 200, "confidence": 0.85, "reasoning": "mentioned in text", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Equal(t, 1, result.Tier)
	assert.Len(t, result.Answers, 2)
	assert.Equal(t, "Technology", result.Answers[0].Value)
	assert.Equal(t, float64(200), result.Answers[1].Value) // JSON numbers are float64
	assert.Equal(t, 400, result.TokenUsage.InputTokens)
	aiClient.AssertExpectations(t)
}

func TestExtractTier1_EmptyRouted(t *testing.T) {
	ctx := context.Background()
	aiClient := &mockAnthropicClient{}
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

	aiClient := &mockAnthropicClient{}

	// Primer request (first of 2+ items) + 2 direct calls (<=3 items).
	// The primer sends batchItems[0].Params, then executeBatch sends each item directly.
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
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

	aiClient := &mockAnthropicClient{}

	// Summarization (Haiku) call.
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Acme Corp is a tech company expanding into AI services."}},
			Usage:   anthropic.TokenUsage{InputTokens: 500, OutputTokens: 100},
		}, nil).Once()

	// T3 extraction (direct mode, 1 item).
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
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
