package pipeline

import (
	"context"
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

func TestClassifyPhase_DirectMode(t *testing.T) {
	ctx := context.Background()

	// Use URLs that don't match URL-based patterns so LLM classification triggers.
	// Content must be >= 100 chars to bypass the tiny page filter.
	pages := []model.CrawledPage{
		{URL: "https://acme.com/our-company-overview", Title: "Overview", Markdown: testPageContent(0)},
		{URL: "https://acme.com/what-we-believe", Title: "Values", Markdown: testPageContent(1)},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Two pages => direct mode (<=threshold). Primer fires (2 items > 1) + 2 direct calls = 3 total.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "homepage", "confidence": 0.95}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Times(3) // primer + 2 direct calls

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	// Mock returns homepage for all calls, so both pages are classified as homepage.
	assert.Len(t, index[model.PageTypeHomepage], 2)
	// Primer(100) + 2 direct(100 each) = 300 input tokens.
	assert.Equal(t, 300, usage.InputTokens)
	assert.Equal(t, 60, usage.OutputTokens)
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
		{"[perplexity_intel] Acme Corp", model.PageTypePerplexityIntel, true},
		{"[Perplexity_Intel] Mixed Case", model.PageTypePerplexityIntel, true},
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

func TestClassifyPhase_DirectMode_PrimerSystemBlocks(t *testing.T) {
	ctx := context.Background()

	// Create 3 pages that need LLM classification (not auto-classifiable)
	pages := []model.CrawledPage{
		{URL: "https://acme.com/our-company-overview", Title: "Overview", Markdown: testPageContent(0)},
		{URL: "https://acme.com/what-we-believe", Title: "Values", Markdown: testPageContent(1)},
		{URL: "https://acme.com/our-partners-page", Title: "Partners Info", Markdown: testPageContent(2)},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// With 3 pages and threshold=20, this goes through direct mode.
	// 3 pages > 1 => primer fires. Total calls: 1 primer + 3 direct = 4.
	var callCount int
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		callCount++
		// All calls should use the classify system prompt
		if len(req.System) > 0 {
			assert.Contains(t, req.System[0].Text, "Classify web pages")
		}
		return true
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
	}, nil).Times(4) // 1 primer + 3 direct

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.NotEmpty(t, index)
	// 4 calls: primer + 3 direct
	assert.Equal(t, 4, callCount)
	// Primer(100) + 3 direct(100 each) = 400
	assert.Equal(t, 400, usage.InputTokens)
	aiClient.AssertExpectations(t)
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

func TestDeduplicatePages_NoDuplicates(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://example.com", Markdown: "Home page content"},
		{URL: "https://example.com/about", Markdown: "About us page"},
		{URL: "https://example.com/services", Markdown: "Services we offer"},
	}

	unique, dupes := deduplicatePages(pages)
	assert.Len(t, unique, 3)
	assert.Empty(t, dupes)
}

func TestDeduplicatePages_WithDuplicates(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://example.com", Markdown: "Shared content"},
		{URL: "https://example.com/about", Markdown: "About us page"},
		{URL: "https://example.com/about-us", Markdown: "Shared content"}, // Duplicate of first.
		{URL: "https://example.com/services", Markdown: "Services page"},
		{URL: "https://example.com/our-services", Markdown: "Services page"}, // Duplicate.
	}

	unique, dupes := deduplicatePages(pages)
	assert.Len(t, unique, 3) // 3 unique
	assert.Len(t, dupes, 2)  // 2 first-URLs have duplicates
	assert.Len(t, dupes["https://example.com"], 1)
	assert.Equal(t, "https://example.com/about-us", dupes["https://example.com"][0].URL)
	assert.Len(t, dupes["https://example.com/services"], 1)
}

func TestDeduplicatePages_AllSame(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://example.com/a", Markdown: "same"},
		{URL: "https://example.com/b", Markdown: "same"},
		{URL: "https://example.com/c", Markdown: "same"},
	}

	unique, dupes := deduplicatePages(pages)
	assert.Len(t, unique, 1)
	assert.Len(t, dupes["https://example.com/a"], 2)
}

func TestDeduplicatePages_Empty(t *testing.T) {
	unique, dupes := deduplicatePages(nil)
	assert.Nil(t, unique)
	assert.Empty(t, dupes)
}

func TestClassifyPhase_TinyPageAutoClassified(t *testing.T) {
	ctx := context.Background()

	// Page with < 100 chars markdown should be auto-classified as "other"
	// without any LLM call.
	pages := []model.CrawledPage{
		{URL: "https://acme.com/stub-page", Title: "Stub", Markdown: "Short content."},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// No LLM calls should be made for tiny pages.

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeOther], 1)
	assert.Equal(t, "https://acme.com/stub-page", index[model.PageTypeOther][0].URL)
	assert.Equal(t, 1.0, index[model.PageTypeOther][0].Classification.Confidence)
	assert.Equal(t, 0, usage.InputTokens)
	aiClient.AssertNotCalled(t, "CreateMessage", mock.Anything, mock.Anything)
}

func TestClassifyPhase_URLPatternAutoClassified(t *testing.T) {
	ctx := context.Background()

	// Pages with URLs matching known patterns should be auto-classified
	// by URL without needing LLM.
	pages := []model.CrawledPage{
		{URL: "https://acme.com/about", Title: "About Us", Markdown: testPageContent(0)},
		{URL: "https://acme.com/contact", Title: "Contact", Markdown: testPageContent(1)},
		{URL: "https://acme.com/services", Title: "Services", Markdown: testPageContent(2)},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// No LLM calls expected — all auto-classified by URL pattern.

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeAbout], 1)
	assert.Len(t, index[model.PageTypeContact], 1)
	assert.Len(t, index[model.PageTypeServices], 1)
	// URL-pattern auto-classification uses confidence 0.9.
	assert.InDelta(t, 0.9, index[model.PageTypeAbout][0].Classification.Confidence, 0.001)
	assert.Equal(t, 0, usage.InputTokens)
	aiClient.AssertNotCalled(t, "CreateMessage", mock.Anything, mock.Anything)
}

func TestClassifyPhase_GroupedPath(t *testing.T) {
	ctx := context.Background()

	// Create 5 pages that need LLM classification (not auto-classifiable by
	// URL pattern or prefix, and content >= 100 chars). With > 3 pages,
	// ClassifyPhase uses the grouped classification path.
	pages := make([]model.CrawledPage, 5)
	for i := 0; i < 5; i++ {
		pages[i] = model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/custom-section-%d", i),
			Title:    fmt.Sprintf("Custom %d", i),
			Markdown: testPageContent(i),
		}
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Build the expected grouped JSON array response.
	pageTypes := []string{"about", "services", "homepage", "contact", "other"}
	var jsonParts []string
	for i := 0; i < 5; i++ {
		jsonParts = append(jsonParts,
			fmt.Sprintf(`{"url":"https://acme.com/custom-section-%d","page_type":"%s","confidence":0.88}`, i, pageTypes[i]))
	}
	groupedJSON := "[" + strings.Join(jsonParts, ",") + "]"

	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: groupedJSON}},
			Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	assert.NoError(t, err)
	assert.Len(t, index[model.PageTypeAbout], 1)
	assert.Len(t, index[model.PageTypeServices], 1)
	assert.Len(t, index[model.PageTypeHomepage], 1)
	assert.Len(t, index[model.PageTypeContact], 1)
	assert.Len(t, index[model.PageTypeOther], 1)
	assert.Equal(t, 300, usage.InputTokens)
	assert.Equal(t, 60, usage.OutputTokens)
	aiClient.AssertExpectations(t)
}

func TestParseGroupedClassification_ValidJSON(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/page1"},
		{URL: "https://acme.com/page2"},
	}

	text := `[{"url":"https://acme.com/page1","page_type":"about","confidence":0.9},{"url":"https://acme.com/page2","page_type":"services","confidence":0.85}]`
	result := parseGroupedClassification(text, pages)

	assert.Equal(t, model.PageTypeAbout, result["https://acme.com/page1"].PageType)
	assert.InDelta(t, 0.9, result["https://acme.com/page1"].Confidence, 0.001)
	assert.Equal(t, model.PageTypeServices, result["https://acme.com/page2"].PageType)
}

func TestParseGroupedClassification_InvalidJSON(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/page1"},
	}

	// Completely invalid JSON should default all pages to "other".
	result := parseGroupedClassification("not json at all", pages)

	assert.Equal(t, model.PageTypeOther, result["https://acme.com/page1"].PageType)
	assert.Equal(t, 0.0, result["https://acme.com/page1"].Confidence)
}

func TestParseGroupedClassification_InvalidPageType(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/page1"},
	}

	text := `[{"url":"https://acme.com/page1","page_type":"nonexistent_type","confidence":0.8}]`
	result := parseGroupedClassification(text, pages)

	// Invalid page type should be normalized to "other".
	assert.Equal(t, model.PageTypeOther, result["https://acme.com/page1"].PageType)
}

func TestParseGroupedClassification_MissingURL(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/page1"},
		{URL: "https://acme.com/page2"},
	}

	// Response only contains page1; page2 should retain default "other".
	text := `[{"url":"https://acme.com/page1","page_type":"about","confidence":0.9}]`
	result := parseGroupedClassification(text, pages)

	assert.Equal(t, model.PageTypeAbout, result["https://acme.com/page1"].PageType)
	assert.Equal(t, model.PageTypeOther, result["https://acme.com/page2"].PageType)
	assert.Equal(t, 0.0, result["https://acme.com/page2"].Confidence)
}

func TestParseGroupedClassification_WrappedInFence(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/page1"},
	}

	text := "```json\n[{\"url\":\"https://acme.com/page1\",\"page_type\":\"careers\",\"confidence\":0.92}]\n```"
	result := parseGroupedClassification(text, pages)

	assert.Equal(t, model.PageTypeCareers, result["https://acme.com/page1"].PageType)
	assert.InDelta(t, 0.92, result["https://acme.com/page1"].Confidence, 0.001)
}
