package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

func TestClassifyPhase_DirectMode(t *testing.T) {
	ctx := context.Background()

	// Use URLs that don't match URL-based patterns so LLM classification triggers.
	pages := []model.CrawledPage{
		{URL: "https://acme.com/our-company-overview", Title: "Overview", Markdown: "Welcome to Acme"},
		{URL: "https://acme.com/what-we-believe", Title: "Values", Markdown: "About Acme Corp values"},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Two pages => direct mode (<=3). Use mock.Anything for ctx since errgroup wraps it.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "homepage", "confidence": 0.95}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Once()

	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
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
	aiClient := anthropicmocks.NewMockClient(t)
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

func TestClassifyByPrefix(t *testing.T) {
	tests := []struct {
		title    string
		wantType model.PageType
		wantOK   bool
	}{
		{"[bbb] Acme Corp BBB Profile", model.PageTypeBBB, true},
		{"[google_maps] Acme Corp", model.PageTypeGoogleMaps, true},
		{"[sos] Acme Corp Filing", model.PageTypeSoS, true},
		{"[linkedin] Acme Corp", model.PageTypeLinkedIn, true},
		{"[BBB] Case Insensitive", model.PageTypeBBB, true},
		{"[Google_Maps] Mixed Case", model.PageTypeGoogleMaps, true},
		{"About Us - Acme Corp", "", false},
		{"Home", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			pt, ok := classifyByPrefix(tt.title)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantType, pt)
			}
		})
	}
}

func TestClassifyByURL(t *testing.T) {
	tests := []struct {
		url      string
		wantType model.PageType
		wantOK   bool
	}{
		{"https://acme.com", model.PageTypeHomepage, true},
		{"https://acme.com/", model.PageTypeHomepage, true},
		{"https://acme.com/about", model.PageTypeAbout, true},
		{"https://acme.com/about-us", model.PageTypeAbout, true},
		{"https://acme.com/About-Us", model.PageTypeAbout, true}, // case insensitive
		{"https://acme.com/contact", model.PageTypeContact, true},
		{"https://acme.com/services", model.PageTypeServices, true},
		{"https://acme.com/products", model.PageTypeProducts, true},
		{"https://acme.com/pricing", model.PageTypePricing, true},
		{"https://acme.com/careers", model.PageTypeCareers, true},
		{"https://acme.com/jobs", model.PageTypeCareers, true},
		{"https://acme.com/team", model.PageTypeTeam, true},
		{"https://acme.com/leadership", model.PageTypeTeam, true},
		{"https://acme.com/faq", model.PageTypeFAQ, true},
		{"https://acme.com/blog", model.PageTypeBlog, true},
		{"https://acme.com/news", model.PageTypeNews, true},
		{"https://acme.com/testimonials", model.PageTypeTestimonials, true},
		{"https://acme.com/investors", model.PageTypeInvestors, true},
		{"https://acme.com/privacy", model.PageTypeLegal, true},
		{"https://acme.com/terms", model.PageTypeLegal, true},
		// Deep paths: only first segment matches.
		{"https://acme.com/about/team", model.PageTypeAbout, true},
		{"https://acme.com/blog/my-post", model.PageTypeBlog, true},
		// No match.
		{"https://acme.com/custom-page", "", false},
		{"https://acme.com/page1", "", false},
		{"https://acme.com/our-company-overview", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			pt, ok := classifyByURL(tt.url)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantType, pt)
			}
		})
	}
}

func TestClassifyPhase_AutoClassifiesExternalPages(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme"},
		{URL: "https://bbb.org/acme", Title: "[bbb] Acme Corp BBB", Markdown: "BBB Profile data"},
		{URL: "https://maps.google.com/acme", Title: "[google_maps] Acme Corp", Markdown: "4.6 stars"},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// All 3 pages are auto-classified: Home by URL pattern (/ → homepage),
	// BBB and Google Maps by title prefix. No LLM calls needed.

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeHomepage], 1)
	assert.Len(t, index[model.PageTypeBBB], 1)
	assert.Len(t, index[model.PageTypeGoogleMaps], 1)
	// No LLM calls — all pages auto-classified.
	assert.Equal(t, 0, usage.InputTokens)
	aiClient.AssertNotCalled(t, "CreateMessage", mock.Anything, mock.Anything)
}

func TestClassifyPhase_AllExternalSkipsLLM(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://bbb.org/acme", Title: "[bbb] Acme Corp", Markdown: "BBB data"},
		{URL: "https://maps.google.com/acme", Title: "[google_maps] Acme Corp", Markdown: "Maps data"},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// No LLM calls expected.

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeBBB], 1)
	assert.Len(t, index[model.PageTypeGoogleMaps], 1)
	assert.Equal(t, 0, usage.InputTokens) // No LLM tokens used.
	aiClient.AssertNotCalled(t, "CreateMessage", mock.Anything, mock.Anything)
}
