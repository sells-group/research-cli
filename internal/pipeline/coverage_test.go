package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	firecrawlmocks "github.com/sells-group/research-cli/pkg/firecrawl/mocks"
	"github.com/sells-group/research-cli/pkg/google"
	googlemocks "github.com/sells-group/research-cli/pkg/google/mocks"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	"github.com/sells-group/research-cli/pkg/ppp"
	salesforcemocks "github.com/sells-group/research-cli/pkg/salesforce/mocks"
)

// ============================================================
// extract.go — uncovered branches
// ============================================================

// TestExtractTier1_WithPPPAndMetadataAndPreSeeded tests that PPP context,
// page metadata, and pre-seeded CSV data are injected into T1 prompts.
func TestExtractTier1_WithPPPAndMetadataAndPreSeeded(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Employee count?", FieldKey: "employee_count", OutputFormat: "number"},
			Pages: []model.ClassifiedPage{
				{
					CrawledPage: model.CrawledPage{
						URL:      "https://acme.com/about",
						Markdown: "We have 200 employees.",
						Metadata: &model.PageMetadata{Rating: 4.5, ReviewCount: 120, BBBRating: "A+"},
					},
				},
			},
		},
	}

	pppMatches := []ppp.LoanMatch{
		{
			BorrowerName:    "Acme Corp",
			CurrentApproval: 150000,
			JobsReported:    50,
			NAICSCode:       "541511",
			BusinessType:    "LLC",
			BusinessAge:     "Existing or more than 2 years old",
			DateApproved:    time.Date(2020, 4, 15, 0, 0, 0, 0, time.UTC),
			LoanStatus:      "Paid in Full",
		},
	}

	company := model.Company{
		PreSeeded: map[string]any{
			"employee_count": 200,
			"naics_code":     "541511",
			"description":    "Tech company",
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Single question: no primer, 1 direct call.
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		// Verify PPP, metadata, and pre-seeded contexts are in the prompt.
		prompt := req.Messages[0].Content
		return strings.Contains(prompt, "PPP Loan Record") &&
			strings.Contains(prompt, "Structured Metadata") &&
			strings.Contains(prompt, "Industry Data")
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"value": 200, "confidence": 0.95, "reasoning": "found", "source_url": "https://acme.com/about"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, company, pppMatches, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Len(t, result.Answers, 1)
	assert.Equal(t, float64(200), result.Answers[0].Value)
}

// TestExtractTier1_RichPromptMultiFieldWithExternalSnippets tests the rich
// prompt path with multi-field questions and external source snippets from
// secondary pages.
func TestExtractTier1_RichPromptMultiFieldWithExternalSnippets(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q1",
				Text:         "Contact info",
				FieldKey:     "phone, email, address",
				OutputFormat: `{"phone":"string","email":"string","address":"string"}`,
				Instructions: "Extract all contact information from the page.",
			},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/contact", Markdown: "Call us at 555-1234. Email: info@acme.com. 123 Main St."}},
				{CrawledPage: model.CrawledPage{URL: "https://bbb.org/acme", Title: "[bbb] Acme Corp", Markdown: "BBB data: phone 555-1234"}},
			},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"phone":"555-1234","email":"info@acme.com","address":"123 Main St","confidence":0.9,"reasoning":"found on page","source_url":"https://acme.com/contact"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 250, OutputTokens: 55},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, model.Company{}, nil, aiClient, aiCfg)

	require.NoError(t, err)
	// Multi-field: one answer per field key.
	assert.Len(t, result.Answers, 3)
	fieldKeys := map[string]bool{}
	for _, a := range result.Answers {
		fieldKeys[a.FieldKey] = true
	}
	assert.True(t, fieldKeys["phone"])
	assert.True(t, fieldKeys["email"])
	assert.True(t, fieldKeys["address"])
}

// TestExtractTier3_WithPPPAndMetadataAndPreSeeded tests that T3 injects
// PPP context, page metadata, and pre-seeded data into the summary context.
func TestExtractTier3_WithPPPAndMetadataAndPreSeeded(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Strategic direction?", FieldKey: "strategy", OutputFormat: "string"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com"}}},
		},
	}

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Acme Corp builds AI."},
	}

	pppMatches := []ppp.LoanMatch{
		{BorrowerName: "Acme Corp", CurrentApproval: 100000, LoanStatus: "Paid in Full"},
	}

	company := model.Company{
		PreSeeded: map[string]any{
			"revenue_range":   "$10M-$50M",
			"year_founded":    "2015",
			"email":           "info@acme.com",
			"exec_first_name": "John",
			"exec_last_name":  "Smith",
			"exec_title":      "CEO",
		},
	}

	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Technology", Confidence: 0.8},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// First call: Haiku summarization (prepareTier3Context).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Company summary: AI startup."}},
			Usage:   anthropic.TokenUsage{InputTokens: 400, OutputTokens: 80},
		}, nil).Once()

	// Second call: T3 extraction (single question, direct mode, no primer since < 3 items).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "AI-first strategy", "confidence": 0.85, "reasoning": "analysis", "source_url": "https://acme.com"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 500, OutputTokens: 100},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{
		HaikuModel: "claude-haiku-4-5-20251001",
		OpusModel:  "claude-opus-4-6",
	}

	result, err := ExtractTier3(ctx, routed, allAnswers, pages, company, pppMatches, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Len(t, result.Answers, 1)
	assert.Equal(t, "AI-first strategy", result.Answers[0].Value)
}

// TestPrepareTier3Context_MergeFallback tests the fallback path when the
// merge summarization call fails: it concatenates partial summaries instead.
func TestPrepareTier3Context_MergeFallback(t *testing.T) {
	ctx := context.Background()

	// Create enough pages to produce multiple chunks (>15K chars total).
	var pages []model.CrawledPage
	for i := 0; i < 10; i++ {
		pages = append(pages, model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/page%d", i),
			Title:    fmt.Sprintf("Page %d", i),
			Markdown: strings.Repeat("B", 4000),
		})
	}

	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech", Confidence: 0.9},
	}

	callCount := 0
	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(func(_ context.Context, _ anthropic.MessageRequest) *anthropic.MessageResponse {
			callCount++
			// Chunk summaries succeed.
			return &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: fmt.Sprintf("Summary chunk %d", callCount)}},
				Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
			}
		}, func(_ context.Context, _ anthropic.MessageRequest) error {
			// Fail on the merge call (the last call).
			if callCount > 3 {
				return errors.New("merge call failed")
			}
			return nil
		})

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	summary, usage, err := prepareTier3Context(ctx, pages, answers, aiClient, aiCfg)

	require.NoError(t, err)
	// Fallback: concatenated summaries instead of merged output.
	assert.NotEmpty(t, summary)
	assert.Greater(t, usage.InputTokens, 0)
}

// TestPrepareTier3Context_ExternalPagesPrioritized verifies that external
// pages (BBB, Google Maps) are placed before regular pages in the chunk order.
func TestPrepareTier3Context_ExternalPagesPrioritized(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme Corp."},
		{URL: "https://bbb.org/acme", Title: "[bbb] Acme BBB Profile", Markdown: "A+ rated business."},
		{URL: "https://acme.com/about", Title: "About", Markdown: "About Acme."},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Single chunk (small total content), single summarization call.
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		prompt := req.Messages[0].Content
		// BBB page should appear before the Home page in the prompt.
		bbbIdx := strings.Index(prompt, "[bbb] Acme BBB Profile")
		homeIdx := strings.Index(prompt, "Home")
		return bbbIdx >= 0 && homeIdx >= 0 && bbbIdx < homeIdx
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: "Summary with BBB data first."}},
		Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	summary, _, err := prepareTier3Context(ctx, pages, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, "Summary with BBB data first.", summary)
}

// TestExtractTier2_RichPromptMultiField tests T2 with the rich prompt template
// for multi-field questions that have Instructions.
func TestExtractTier2_RichPromptMultiField(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q1",
				Text:         "Leadership team",
				FieldKey:     "ceo_name, cfo_name, coo_name",
				OutputFormat: `{"ceo_name":"string","cfo_name":"string","coo_name":"string"}`,
				Instructions: "Extract executive team names from all pages.",
			},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/team", Markdown: "CEO: Alice, CFO: Bob, COO: Carol"}},
			},
		},
	}

	t1Answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Single question: no primer, 1 direct call.
	aiClient.On("CreateMessage", mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		// Rich prompt uses the richSystemText system prompt.
		for _, sys := range req.System {
			if strings.Contains(sys.Text, "structured data") {
				return true
			}
		}
		return false
	})).Return(&anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{{Text: `{"ceo_name":"Alice","cfo_name":"Bob","coo_name":"Carol","confidence":0.92,"reasoning":"listed on team page","source_url":"https://acme.com/team"}`}},
		Usage:   anthropic.TokenUsage{InputTokens: 350, OutputTokens: 70},
	}, nil).Once()

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, model.Company{}, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, 2, result.Tier)
	assert.Len(t, result.Answers, 3) // One per field key.
}

// TestExtractTier3_RichPromptMultiField tests T3 with rich prompt template.
func TestExtractTier3_RichPromptMultiField(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{
				ID:           "q1",
				Text:         "Financials",
				FieldKey:     "revenue, profit_margin",
				OutputFormat: `{"revenue":"string","profit_margin":"string"}`,
				Instructions: "Extract financial data from all available context.",
			},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/investors", Markdown: "Revenue $50M, Margin 15%"}},
			},
		},
	}

	pages := []model.CrawledPage{
		{URL: "https://acme.com/investors", Title: "Investors", Markdown: "Revenue $50M, Margin 15%"},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// First call: summarization.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Summary: Revenue $50M, Profit Margin 15%."}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Once()

	// Second call: T3 extraction (rich prompt, 1 item, direct, no primer < 3).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"revenue":"$50M","profit_margin":"15%","confidence":0.9,"reasoning":"investor page","source_url":"https://acme.com/investors"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 400, OutputTokens: 80},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{
		HaikuModel: "claude-haiku-4-5-20251001",
		OpusModel:  "claude-opus-4-6",
	}

	result, err := ExtractTier3(ctx, routed, nil, pages, model.Company{}, nil, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, 3, result.Tier)
	assert.Len(t, result.Answers, 2)
}

// ============================================================
// classify.go — uncovered branches
// ============================================================

// TestClassifyPhase_DeduplicationInheritsClassification tests that
// deduplicated pages inherit the classification of their content twin
// after LLM classification.
func TestClassifyPhase_DeduplicationInheritsClassification(t *testing.T) {
	ctx := context.Background()

	// Three pages, two with identical content. All need LLM (no URL pattern match,
	// no prefix match, content >= 100 chars).
	pages := []model.CrawledPage{
		{URL: "https://acme.com/our-company-overview", Title: "Overview", Markdown: testPageContent(10)},
		{URL: "https://acme.com/about-page", Title: "About Copy", Markdown: testPageContent(10)}, // Same content as first.
		{URL: "https://acme.com/what-we-believe", Title: "Values", Markdown: testPageContent(11)},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// Only 2 unique pages go to LLM (grouped path since > 3 is false, so direct/batch).
	// Actually 2 unique pages <= 3, so classifyDirect path. Primer (2 > 1) + 2 direct = 3 calls.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil).Times(3) // primer + 2 direct

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, _, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	require.NoError(t, err)
	// All 3 pages should be classified as "about" (the duplicate inherits).
	assert.Len(t, index[model.PageTypeAbout], 3)
}

// TestClassifyByURL_InvalidURL tests that classifyByURL returns false
// for a URL that fails to parse.
func TestClassifyByURL_InvalidURL(t *testing.T) {
	pt, ok := classifyByURL("://invalid")
	assert.False(t, ok)
	assert.Equal(t, model.PageType(""), pt)
}

// TestClassifyBatch_CreateBatchError tests the error path when
// CreateBatch fails in classifyBatch.
func TestClassifyBatch_CreateBatchError(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com/page0", Markdown: "Content 0"},
	}
	items := []anthropic.BatchRequestItem{
		{CustomID: "classify-0", Params: anthropic.MessageRequest{Model: "haiku", MaxTokens: 128}},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateBatch", ctx, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(nil, errors.New("batch api down"))

	usage := &model.TokenUsage{}
	index, _, err := classifyBatch(ctx, pages, items, aiClient, usage)

	assert.Nil(t, index)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "classify: create batch")
}

// TestClassifyPhase_MixedAutoAndLLM tests the path where some pages are
// auto-classified by prefix/URL and others require LLM classification,
// ensuring both sets merge into the final index.
func TestClassifyPhase_MixedAutoAndLLM(t *testing.T) {
	ctx := context.Background()

	pages := []model.CrawledPage{
		// Auto-classified by title prefix.
		{URL: "https://bbb.org/acme", Title: "[bbb] Acme Corp", Markdown: "BBB data here"},
		// Auto-classified by URL pattern.
		{URL: "https://acme.com/about", Title: "About Us", Markdown: testPageContent(20)},
		// Tiny page auto-classified as other.
		{URL: "https://acme.com/empty", Title: "Empty", Markdown: "Short."},
		// LLM-needed pages (> 3 triggers grouped path).
		{URL: "https://acme.com/custom-a", Title: "Custom A", Markdown: testPageContent(21)},
		{URL: "https://acme.com/custom-b", Title: "Custom B", Markdown: testPageContent(22)},
		{URL: "https://acme.com/custom-c", Title: "Custom C", Markdown: testPageContent(23)},
		{URL: "https://acme.com/custom-d", Title: "Custom D", Markdown: testPageContent(24)},
	}

	aiClient := anthropicmocks.NewMockClient(t)

	// 4 LLM pages => grouped path (> 3), 1 group call.
	var jsonParts []string
	for i, suffix := range []string{"custom-a", "custom-b", "custom-c", "custom-d"} {
		types := []string{"services", "products", "careers", "homepage"}
		jsonParts = append(jsonParts,
			fmt.Sprintf(`{"url":"https://acme.com/%s","page_type":"%s","confidence":0.88}`, suffix, types[i]))
	}
	groupedJSON := "[" + strings.Join(jsonParts, ",") + "]"

	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: groupedJSON}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 40},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	index, usage, err := ClassifyPhase(ctx, pages, aiClient, aiCfg)

	require.NoError(t, err)
	// Auto-classified pages.
	assert.Len(t, index[model.PageTypeBBB], 1)
	assert.Len(t, index[model.PageTypeAbout], 1)
	assert.Len(t, index[model.PageTypeOther], 1)
	// LLM-classified pages.
	assert.Len(t, index[model.PageTypeServices], 1)
	assert.Len(t, index[model.PageTypeProducts], 1)
	assert.Len(t, index[model.PageTypeCareers], 1)
	assert.Len(t, index[model.PageTypeHomepage], 1)
	// LLM usage tracked.
	assert.Equal(t, 200, usage.InputTokens)
}

// ============================================================
// crawl.go — uncovered branches
// ============================================================

// TestCrawlPhase_ProbePassedIn_EmptyURLsFallsToFirecrawl tests the path
// where a pre-computed probe is passed in, link discovery returns empty URLs,
// and the code falls back to firecrawl.
func TestCrawlPhase_ProbePassedIn_EmptyURLsFallsToFirecrawl(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://empty-links.com", Name: "EmptyLinks"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, "https://empty-links.com").Return(nil, nil)

	fcClient := firecrawlmocks.NewMockClient(t)
	fcClient.On("Crawl", ctx, firecrawl.CrawlRequest{
		URL:   "https://empty-links.com",
		Limit: 50,
	}).Return(&firecrawl.CrawlResponse{ID: "crawl-empty"}, nil)
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-empty").
		Return(&firecrawl.CrawlStatusResponse{
			Status: "completed",
			Data: []firecrawl.PageData{
				{URL: "https://empty-links.com", Title: "Home", Markdown: "Welcome", StatusCode: 200},
			},
		}, nil)
	st.On("SetCachedCrawl", ctx, "https://empty-links.com", mock.AnythingOfType("[]model.CrawledPage"), 24*time.Hour).
		Return(nil)

	chain := testChain(t, newTestScraper(t, "s1", true, nil, nil))
	cfg := config.CrawlConfig{}

	// Pass a non-blocked, reachable probe. DiscoverLinks will likely fail
	// or return empty for a non-existent URL, triggering the firecrawl fallback.
	probe := &model.ProbeResult{Reachable: true, Blocked: false, FinalURL: "https://empty-links.com"}
	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, probe)

	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	assert.Len(t, result.Pages, 1)
}

// TestCrawlPhase_DefaultMaxPagesAndDepth tests that zero MaxPages and
// MaxDepth default to 50 and 2 respectively.
func TestCrawlPhase_DefaultMaxPagesAndDepth(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://defaults.com", Name: "Defaults"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, "https://defaults.com").Return(nil, nil)

	fcClient := firecrawlmocks.NewMockClient(t)
	// When falling back to firecrawl, should use default MaxPages=50.
	fcClient.On("Crawl", ctx, firecrawl.CrawlRequest{
		URL:   "https://defaults.com",
		Limit: 50,
	}).Return(&firecrawl.CrawlResponse{ID: "crawl-defaults"}, nil)
	fcClient.On("GetCrawlStatus", mock.Anything, "crawl-defaults").
		Return(&firecrawl.CrawlStatusResponse{
			Status: "completed",
			Data: []firecrawl.PageData{
				{URL: "https://defaults.com", Title: "Home", Markdown: "Welcome", StatusCode: 200},
			},
		}, nil)
	st.On("SetCachedCrawl", ctx, "https://defaults.com", mock.AnythingOfType("[]model.CrawledPage"), 24*time.Hour).
		Return(nil)

	chain := testChain(t, newTestScraper(t, "s1", true, nil, nil))
	// All zeros to test default values.
	cfg := config.CrawlConfig{MaxPages: 0, MaxDepth: 0, CacheTTLHours: 0}

	probe := &model.ProbeResult{Reachable: true, Blocked: true, BlockType: "cloudflare"}
	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, probe)

	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	fcClient.AssertExpectations(t)
}

// ============================================================
// pipeline.go — uncovered branches
// ============================================================

// covTestExporter is a test double for the ResultExporter interface.
// Named differently from mockExporter in exporter_test.go to avoid redeclaration.
type covTestExporter struct {
	name    string
	flushed bool
	err     error
}

func (m *covTestExporter) ExportResult(_ context.Context, _ *model.EnrichmentResult, _ *GateResult) error {
	return nil
}

func (m *covTestExporter) Flush(_ context.Context) error {
	m.flushed = true
	return m.err
}

func (m *covTestExporter) Name() string {
	return m.name
}

// TestPipeline_ExporterByName_Found tests finding a registered exporter.
func TestPipeline_ExporterByName_Found(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	exp := &covTestExporter{name: "csv"}
	p.AddExporter(exp)
	p.AddExporter(&covTestExporter{name: "json"})

	found := p.ExporterByName("csv")
	assert.NotNil(t, found)
	assert.Equal(t, "csv", found.Name())
}

// TestPipeline_ExporterByName_NotFound tests that nil is returned for
// an unregistered exporter name.
func TestPipeline_ExporterByName_NotFound(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	p.AddExporter(&covTestExporter{name: "csv"})

	found := p.ExporterByName("webhook")
	assert.Nil(t, found)
}

// TestPipeline_FlushExporters_WithError tests that FlushExporters returns the
// first error and wraps it with the exporter name.
func TestPipeline_FlushExporters_WithError(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	p.AddExporter(&covTestExporter{name: "csv", err: nil})
	p.AddExporter(&covTestExporter{name: "json", err: errors.New("write failed")})

	err := p.FlushExporters(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json")
	assert.Contains(t, err.Error(), "write failed")
}

// TestPipeline_FlushExporters_AllSucceed tests the happy path.
func TestPipeline_FlushExporters_AllSucceed(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	exp1 := &covTestExporter{name: "csv"}
	exp2 := &covTestExporter{name: "json"}
	p.AddExporter(exp1)
	p.AddExporter(exp2)

	err := p.FlushExporters(context.Background())
	assert.NoError(t, err)
	assert.True(t, exp1.flushed)
	assert.True(t, exp2.flushed)
}

// TestPipeline_SetFedsyncPool tests setting the optional pool.
func TestPipeline_SetFedsyncPool(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	assert.Nil(t, p.fedsyncPool)
	// SetFedsyncPool with nil is valid.
	p.SetFedsyncPool(nil)
	assert.Nil(t, p.fedsyncPool)
}

// TestPipeline_SetForceReExtract tests toggling force re-extraction.
func TestPipeline_SetForceReExtract(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}}
	assert.False(t, p.forceReExtract)
	p.SetForceReExtract(true)
	assert.True(t, p.forceReExtract)
}

// ============================================================
// Additional extract.go edge cases
// ============================================================

// TestExtractTier2_WithPrimerAndPPP tests the T2 primer path (2+ items,
// NoBatch=false) and PPP/metadata/pre-seeded injection.
func TestExtractTier2_WithPrimerAndPPP(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "Revenue?", FieldKey: "revenue", OutputFormat: "string"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{
					URL:      "https://acme.com/about",
					Markdown: "Revenue was $10M in 2024",
					Metadata: &model.PageMetadata{Rating: 4.2, ReviewCount: 80},
				}},
			},
		},
		{
			Question: model.Question{ID: "q2", Text: "Employee count?", FieldKey: "employees", OutputFormat: "number"},
			Pages: []model.ClassifiedPage{
				{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "200 employees"}},
			},
		},
	}

	t1Answers := []model.ExtractionAnswer{
		{QuestionID: "q0", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}

	pppMatches := []ppp.LoanMatch{
		{BorrowerName: "Acme Corp", JobsReported: 50, NAICSCode: "541511"},
	}

	company := model.Company{
		PreSeeded: map[string]any{"employee_count": 200},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// 2 items > 1, NoBatch=false: primer fires. primer + 2 direct = 3 calls.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": "$10.5M", "confidence": 0.92, "reasoning": "found", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60, CacheCreationInputTokens: 100, CacheReadInputTokens: 50},
		}, nil).Times(3)

	aiCfg := config.AnthropicConfig{SonnetModel: "claude-sonnet-4-5-20250929"}

	result, err := ExtractTier2(ctx, routed, t1Answers, company, pppMatches, aiClient, aiCfg)

	require.NoError(t, err)
	assert.Equal(t, 2, result.Tier)
	assert.Len(t, result.Answers, 2)
	// Primer tokens should be counted.
	assert.Greater(t, result.TokenUsage.CacheCreationTokens, 0)
	assert.Greater(t, result.TokenUsage.CacheReadTokens, 0)
}

// TestPrepareTier3Context_ChunkSummarizeFails tests that individual chunk
// summarization failures are handled gracefully (the group continues).
func TestPrepareTier3Context_ChunkSummarizeFails(t *testing.T) {
	ctx := context.Background()

	// Create enough pages for 2+ chunks to trigger parallel summarization.
	var pages []model.CrawledPage
	for i := 0; i < 12; i++ {
		pages = append(pages, model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/page%d", i),
			Title:    fmt.Sprintf("Page %d", i),
			Markdown: strings.Repeat("C", 4000),
		})
	}

	callIdx := 0
	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(func(_ context.Context, _ anthropic.MessageRequest) *anthropic.MessageResponse {
			callIdx++
			return &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: fmt.Sprintf("Chunk summary %d", callIdx)}},
				Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
			}
		}, func(_ context.Context, _ anthropic.MessageRequest) error {
			// First chunk call fails.
			if callIdx == 1 {
				return errors.New("rate limited")
			}
			return nil
		})

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	summary, usage, err := prepareTier3Context(ctx, pages, nil, aiClient, aiCfg)

	require.NoError(t, err)
	// Summary should still exist from the non-failing chunks + merge.
	assert.NotEmpty(t, summary)
	assert.Greater(t, usage.InputTokens, 0)
}

// TestExtractTier1_NoPagesSkipsQuestion tests that a routed question
// with zero pages produces no batch item (the continue branch is hit).
func TestExtractTier1_NoPagesSkipsQuestion(t *testing.T) {
	ctx := context.Background()

	routed := []model.RoutedQuestion{
		{
			Question: model.Question{ID: "q1", Text: "What industry?", FieldKey: "industry", OutputFormat: "string"},
			Pages:    nil, // No pages matched — will be skipped in batch item creation.
		},
		{
			Question: model.Question{ID: "q2", Text: "How many employees?", FieldKey: "employees", OutputFormat: "number"},
			Pages:    []model.ClassifiedPage{{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "200 employees"}}},
		},
	}

	aiClient := anthropicmocks.NewMockClient(t)
	// Only 1 batch item created (for q2), so 1 direct call, no primer.
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"value": 200, "confidence": 0.9, "reasoning": "found", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil).Once()

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	result, err := ExtractTier1(ctx, routed, model.Company{}, nil, aiClient, aiCfg)

	require.NoError(t, err)
	// Exactly 1 answer since q1 had no pages (skipped in batch item creation).
	assert.Len(t, result.Answers, 1)
	// Only 1 API call was made (no primer for single item).
	aiClient.AssertNumberOfCalls(t, "CreateMessage", 1)
}

// TestPrepareTier3Context_SingleValidSummary tests the path where parallel
// chunk summarization produces only one valid summary (others fail), so it
// returns that single summary directly without merging.
func TestPrepareTier3Context_SingleValidSummary(t *testing.T) {
	ctx := context.Background()

	// Create pages for 2+ chunks.
	var pages []model.CrawledPage
	for i := 0; i < 12; i++ {
		pages = append(pages, model.CrawledPage{
			URL:      fmt.Sprintf("https://acme.com/page%d", i),
			Title:    fmt.Sprintf("Page %d", i),
			Markdown: strings.Repeat("D", 4000),
		})
	}

	callNum := 0
	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(func(_ context.Context, _ anthropic.MessageRequest) *anthropic.MessageResponse {
			callNum++
			// Only the first chunk call succeeds.
			if callNum == 1 {
				return &anthropic.MessageResponse{
					Content: []anthropic.ContentBlock{{Text: "Only valid summary"}},
					Usage:   anthropic.TokenUsage{InputTokens: 300, OutputTokens: 60},
				}
			}
			return &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: ""}},
				Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 10},
			}
		}, func(_ context.Context, _ anthropic.MessageRequest) error {
			// All but the first chunk fail.
			if callNum > 1 {
				return errors.New("api error")
			}
			return nil
		})

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	summary, _, err := prepareTier3Context(ctx, pages, nil, aiClient, aiCfg)

	require.NoError(t, err)
	// With only 1 valid summary, it returns directly without merging.
	assert.Equal(t, "Only valid summary", summary)
}

// ============================================================
// Additional tests targeting uncovered blocks across files
// ============================================================

// TestFormatComparisonReport_AllMatchTypes tests FormatComparisonReport with
// every match type (close, high_conf, canonical, naics) plus zero-total paths.
func TestFormatComparisonReport_AllMatchTypes(t *testing.T) {
	comparisons := []CompanyComparison{
		{
			Domain:      "acme.com",
			CompanyName: "Acme Corp",
			MatchRate:   0.75,
			Comparisons: []FieldComparison{
				{Field: "naics_code", GrataValue: "541511", OurValue: "541511", Match: true, Proximity: 1.0, MatchType: "exact", Confidence: 0.9},
				{Field: "employee_count", GrataValue: "100", OurValue: "105", Match: true, Proximity: 0.95, MatchType: "close", Confidence: 0.85},
				{Field: "employee_count", GrataValue: "200", OurValue: "150", Match: true, Proximity: 0.5, MatchType: "high_conf", Confidence: 0.92},
				{Field: "business_model", GrataValue: "B2B", OurValue: "business-to-business", Match: true, Proximity: 1.0, MatchType: "canonical", Confidence: 0.88},
				{Field: "phone", GrataValue: "8018747020", OurValue: "(801) 874-7020", Match: true, Proximity: 1.0, MatchType: "format"},
				{Field: "description", GrataValue: "A tech company", OurValue: "", Match: false, Proximity: 0, MatchType: "gap"},
				{Field: "exec_title", GrataValue: "CEO", OurValue: "CTO", Match: false, Proximity: 0.3, MatchType: "wrong", Confidence: 0.7},
				{Field: "review_count", GrataValue: "50", OurValue: "55", Match: true, Proximity: 0.9, MatchType: "close"},
			},
		},
		{
			Domain:      "empty.com",
			CompanyName: "Empty Corp",
			MatchRate:   0,
			Comparisons: nil, // zero-total path
		},
	}

	report := FormatComparisonReport(comparisons)

	assert.Contains(t, report, "~0.95")          // close match
	assert.Contains(t, report, "OK (high_conf")  // high_conf match
	assert.Contains(t, report, "OK (canonical)") // canonical match
	assert.Contains(t, report, "N/A")            // zero-total path
	assert.Contains(t, report, "WRONG (0.30)")   // wrong with proximity
	assert.Contains(t, report, "--- SUMMARY ---")
}

// TestParseGrataCSV_CityOnlyAndStateOnly tests location formatting when only
// city or only state is provided.
func TestParseGrataCSV_CityOnlyAndStateOnly(t *testing.T) {
	content := `Domain,Name,City,State
cityonly.com,City Only Corp,DENVER,
stateonly.com,State Only Corp,,TEXAS
both.com,Both Corp,AUSTIN,TEXAS
`
	path := filepath.Join(t.TempDir(), "test_loc.csv")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	companies, err := ParseGrataCSV(path)
	require.NoError(t, err)
	require.Len(t, companies, 3)

	// City only: location = city formatted.
	assert.Equal(t, "Denver", companies[0].Location)
	// State only: location = state abbreviation.
	assert.Equal(t, "TX", companies[1].Location)
	// Both: location = "City, ST".
	assert.Equal(t, "Austin, TX", companies[2].Location)
}

// TestParseGrataCSVFull_CityOnlyAndStateOnly tests location formatting
// in ParseGrataCSVFull when only city or only state is present.
func TestParseGrataCSVFull_CityOnlyAndStateOnly(t *testing.T) {
	content := `Domain,Name,City,State,Description,Revenue Estimate
cityonly.com,City Only,SEATTLE,,desc1,$1M
stateonly.com,State Only,,FLORIDA,desc2,$2M
`
	path := filepath.Join(t.TempDir(), "full_loc.csv")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	companies, err := ParseGrataCSVFull(path)
	require.NoError(t, err)
	require.Len(t, companies, 2)

	// City only.
	assert.Equal(t, "Seattle", companies[0].Location)
	assert.Equal(t, "Seattle", companies[0].City)
	assert.Equal(t, "", companies[0].State) // no state provided

	// State only.
	assert.Equal(t, "FL", companies[1].Location)
	assert.Equal(t, "", companies[1].City) // no city provided
	assert.Equal(t, "FL", companies[1].State)
}

// TestDeriveNameFromPages_NoMatchingTitles tests the fallback path where
// pages exist but none have a useful title, falling back to domainToName.
func TestDeriveNameFromPages_NoMatchingTitles(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "", Markdown: "Content"},
		{URL: "https://acme.com/about", Title: "  ", Markdown: "About content"},
	}

	name := deriveNameFromPages(pages, "https://acme.com")
	// All titles are empty/whitespace-only, so falls back to domainToName.
	assert.Equal(t, "Acme", name)
}

// TestDomainToName_Unparseable tests domainToName with a URL that can't be parsed.
func TestDomainToName_Unparseable(t *testing.T) {
	name := domainToName("")
	assert.Equal(t, "", name)

	// A URL that even with "https://" prefix has no host.
	name = domainToName("://")
	assert.Equal(t, "", name)
}

// TestEscalateQuestions_ZeroTotalAndDuplicate tests the edge cases where
// all answers pass threshold, so nothing escalates.
func TestEscalateQuestions_ZeroTotalAndDuplicate(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.9},
		{QuestionID: "q1", FieldKey: "industry", Value: "Software", Confidence: 0.8},
	}
	questions := []model.Question{
		{ID: "q1", Text: "What industry?", FieldKey: "industry",
			PageTypes: []model.PageType{model.PageTypeAbout}},
	}
	index := model.PageIndex{
		model.PageTypeAbout: []model.ClassifiedPage{
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Markdown: "Tech company."}},
		},
	}

	// High threshold + high fail rate threshold -- nothing escalated.
	escalated := EscalateQuestions(answers, questions, index, 0.4, 0.35)
	assert.Empty(t, escalated)
}

// TestCompareField_PhoneFormatDiff tests compareField phone path where digits
// match but formatting differs.
func TestCompareField_PhoneFormatDiff(t *testing.T) {
	match, prox, mt := compareField("phone", "8015551234", "(801) 555-1234", 0.9)
	assert.True(t, match)
	assert.Equal(t, 1.0, prox)
	assert.Equal(t, "format", mt)
}

// TestCompareField_PhoneExact tests compareField phone path where phone
// numbers are identical strings.
func TestCompareField_PhoneExact(t *testing.T) {
	match, prox, mt := compareField("phone", "(801) 555-1234", "(801) 555-1234", 0.9)
	assert.True(t, match)
	assert.Equal(t, 1.0, prox)
	assert.Equal(t, "exact", mt)
}

// TestCompareField_PhoneWrong tests compareField phone path where digits differ.
func TestCompareField_PhoneWrong(t *testing.T) {
	match, _, mt := compareField("phone", "8015551234", "8015559999", 0.9)
	assert.False(t, match)
	assert.Equal(t, "wrong", mt)
}

// TestCompareField_EmployeeExact tests the exact match path for employee_count.
func TestCompareField_EmployeeExact(t *testing.T) {
	match, prox, mt := compareField("employee_count", "100", "100", 0.9)
	assert.True(t, match)
	assert.Equal(t, 1.0, prox)
	assert.Equal(t, "exact", mt)
}

// TestCompareField_ReviewCountExact tests the exact match path for review_count.
func TestCompareField_ReviewCountExact(t *testing.T) {
	match, _, mt := compareField("review_count", "50", "50", 0.9)
	assert.True(t, match)
	assert.Equal(t, "exact", mt)
}

// TestCompareField_ReviewCountHighConf tests the high-conf path for review_count.
func TestCompareField_ReviewCountHighConf(t *testing.T) {
	// review_count with large difference but high confidence.
	match, _, mt := compareField("review_count", "100", "50", 0.75)
	assert.True(t, match)
	assert.Equal(t, "high_conf", mt)
}

// TestCompareField_GenericNumericExact tests the exact match for a generic
// numeric field (review_rating).
func TestCompareField_GenericNumericExact(t *testing.T) {
	match, prox, mt := compareField("review_rating", "4.5", "4.5", 0.9)
	assert.True(t, match)
	assert.Equal(t, 1.0, prox)
	assert.Equal(t, "exact", mt)
}

// TestCompareField_GenericNumericClose tests a close match for review_rating.
func TestCompareField_GenericNumericClose(t *testing.T) {
	match, _, mt := compareField("review_rating", "4.5", "4.3", 0.9)
	assert.True(t, match)
	assert.Equal(t, "close", mt)
}

// TestCompareField_GenericNumericWrong tests a wrong result for review_rating.
func TestCompareField_GenericNumericWrong(t *testing.T) {
	match, _, mt := compareField("review_rating", "5", "1", 0.9)
	assert.False(t, match)
	assert.Equal(t, "wrong", mt)
}

// TestCompareField_NAICSHierarchical4Digit tests NAICS 4-digit prefix match.
func TestCompareField_NAICSHierarchical4Digit(t *testing.T) {
	match, prox, mt := compareField("naics_code", "541511", "541519", 0.9)
	assert.True(t, match)
	assert.InDelta(t, 0.8, prox, 0.01)
	assert.Equal(t, "close", mt)
}

// TestCompareField_NAICSHierarchical3Digit tests NAICS 3-digit prefix match.
func TestCompareField_NAICSHierarchical3Digit(t *testing.T) {
	// "5415" != "5419" so no 4-digit match; "541" == "541" so 3-digit match.
	match, prox, mt := compareField("naics_code", "541511", "541999", 0.9)
	assert.True(t, match)
	assert.InDelta(t, 0.6, prox, 0.01)
	assert.Equal(t, "close", mt)
}

// TestCompareField_NAICSHierarchical2Digit tests NAICS 2-digit sector match.
func TestCompareField_NAICSHierarchical2Digit(t *testing.T) {
	match, prox, mt := compareField("naics_code", "541511", "549999", 0.9)
	assert.True(t, match)
	assert.InDelta(t, 0.4, prox, 0.01)
	assert.Equal(t, "close", mt)
}

// TestCompareField_DescriptionBigramBoost tests the description comparison
// where bigram overlap boosts the proximity score above the threshold.
func TestCompareField_DescriptionBigramBoost(t *testing.T) {
	grata := "Acme Corp provides solar panel installation services nationwide"
	ours := "Acme Corp is a leading solar panel installer across the United States"
	match, prox, _ := compareField("description", grata, ours, 0.8)
	assert.True(t, match)
	assert.Greater(t, prox, 0.15)
}

// TestCompareField_DescriptionSharedKeywords tests the description comparison
// shared-keywords floor when bigram/jaccard are both low.
func TestCompareField_DescriptionSharedKeywords(t *testing.T) {
	grata := "company specializes in custom manufacturing automotive parts components serving industrial clients"
	ours := "industrial manufacturing company providing custom automotive components precision engineering"
	match, prox, _ := compareField("description", grata, ours, 0.8)
	assert.True(t, match)
	assert.Greater(t, prox, 0.19)
}

// TestCompareField_ExecTitleNormalized tests exec_title comparison where
// titles match after normalization (comma vs ampersand).
func TestCompareField_ExecTitleNormalized(t *testing.T) {
	match, prox, mt := compareField("exec_title", "President, CEO", "President & CEO", 0.9)
	assert.True(t, match)
	assert.Equal(t, 1.0, prox)
	assert.Equal(t, "close", mt)
}

// TestCompareField_ExecTitleWordOverlap tests exec_title comparison with
// word overlap below threshold.
func TestCompareField_ExecTitleWordOverlap(t *testing.T) {
	match, _, _ := compareField("exec_title", "Chief Executive Officer", "CEO and President", 0.9)
	// "chief executive officer" vs "ceo and president" have no word overlap.
	assert.False(t, match)
}

// TestNumericProximity_BothZero tests that numericProximity returns 1 when
// both inputs are zero.
func TestNumericProximity_BothZero(t *testing.T) {
	prox := numericProximity("0", "0")
	assert.Equal(t, 1.0, prox)
}

// TestBigramOverlap_SingleWord tests that bigramOverlap returns 0 when
// inputs have fewer than 2 words.
func TestBigramOverlap_SingleWord(t *testing.T) {
	prox := bigramOverlap([]string{"hello"}, []string{"hello", "world"})
	assert.Equal(t, 0.0, prox)
}

// TestStringOverlap_BothEmpty tests stringOverlap returns 1 for two empty strings.
func TestStringOverlap_BothEmpty(t *testing.T) {
	prox := stringOverlap("", "")
	assert.Equal(t, 1.0, prox)
}

// ============================================================
// CrawlPhase — chain scrape success path (lines 86-109)
// ============================================================

// TestCrawlPhase_ChainScrapeSuccessWithHTTPServer tests the full chain
// scrape success path with a real httptest server, covering lines 86-109
// of crawl.go (ScrapeAll returns pages, cache write, chain result).
func TestCrawlPhase_ChainScrapeSuccessWithHTTPServer(t *testing.T) {
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = fmt.Fprint(w, `<html><head><title>Test Corp</title></head><body><a href="/about">About</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><h1>About Test Corp</h1><p>We are a technology company.</p></body></html>`)
	})
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "User-agent: *\nAllow: /")
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	company := model.Company{URL: srv.URL, Name: "Test Corp"}

	st := storemocks.NewMockStore(t)
	st.On("GetCachedCrawl", ctx, srv.URL).Return(nil, nil) // Cache miss.
	st.On("SetCachedCrawl", ctx, srv.URL, mock.AnythingOfType("[]model.CrawledPage"), 24*time.Hour).Return(nil)

	// Create a scraper that returns content for any URL.
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock-jina").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(func(_ context.Context, pageURL string) *scrape.Result {
		return &scrape.Result{
			Page: model.CrawledPage{
				URL:        pageURL,
				Title:      "Scraped: " + pageURL,
				Markdown:   "Content from " + pageURL,
				StatusCode: 200,
			},
			Source: "mock-jina",
		}
	}, func(_ context.Context, _ string) error {
		return nil
	}).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	fcClient := firecrawlmocks.NewMockClient(t)
	cfg := config.CrawlConfig{MaxPages: 50, MaxDepth: 2}

	// Pass a clean probe so CrawlPhase skips re-probing.
	probe := &model.ProbeResult{
		Reachable: true,
		Blocked:   false,
		FinalURL:  srv.URL,
	}
	result, err := CrawlPhase(ctx, company, cfg, st, chain, fcClient, probe)

	require.NoError(t, err)
	assert.Equal(t, "chain", result.Source)
	assert.False(t, result.FromCache)
	assert.Greater(t, len(result.Pages), 0)
}

// ============================================================
// derive.go — deriveNameFromPages homepage with empty title
// ============================================================

// TestDeriveNameFromPages_HomepageEmptyTitle tests the branch where the
// homepage matches by URL but has an empty cleaned title, falling through
// to the "first page with non-empty title" path.
func TestDeriveNameFromPages_HomepageEmptyTitle(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/", Title: ""},
		{URL: "https://acme.com/about", Title: "About Us | Acme Industries"},
	}

	name := deriveNameFromPages(pages, "https://acme.com")
	assert.Equal(t, "About Us", name) // Falls through to first non-empty title.
}

// TestDeriveNameFromPages_AllEmptyTitles tests that domainToName is used
// when all pages have empty titles.
func TestDeriveNameFromPages_AllEmptyTitles(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/", Title: ""},
		{URL: "https://acme.com/about", Title: ""},
	}

	name := deriveNameFromPages(pages, "https://acme.com")
	assert.Equal(t, "Acme", name) // Falls back to domainToName.
}

// ============================================================
// sfreport_export.go — error path
// ============================================================

// TestExportSFReportCSV_ValidPath tests the full ExportSFReportCSV path
// with a valid output file, covering write header and row branches.
func TestExportSFReportCSV_ValidPath(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "sfreport.csv")
	results := []*model.EnrichmentResult{
		{
			Company: model.Company{Name: "Acme", URL: "https://acme.com", SalesforceID: "001ABC"},
			FieldValues: map[string]model.FieldValue{
				"industry": {FieldKey: "industry", Value: "Tech"},
			},
		},
	}
	originals := []SFReportCompany{
		{AccountID: "001ABC", Ownership: "Private"},
	}
	err := ExportSFReportCSV(results, originals, outPath)
	assert.NoError(t, err)

	data, readErr := os.ReadFile(outPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(data), "Acme")
	assert.Contains(t, string(data), "Private")
}

// ============================================================
// sfreport_export.go — buildSFReportRow with original
// ============================================================

// TestBuildSFReportRow_WithOriginal tests that pass-through fields
// from the original SFReportCompany are included in the row.
func TestBuildSFReportRow_WithOriginal(t *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Acme Corp",
			SalesforceID: "001ABC",
			URL:          "https://acme.com",
			City:         "Springfield",
			State:        "IL",
		},
		FieldValues: map[string]model.FieldValue{
			"year_established": {FieldKey: "year_established", Value: "2010"},
			"employees":        {FieldKey: "employees", Value: "250"},
			"locations":        {FieldKey: "locations", Value: "3"},
			"description":      {FieldKey: "description", Value: "A tech company."},
			"service_mix":      {FieldKey: "service_mix", Value: "Consulting, Dev"},
			"differentiators":  {FieldKey: "differentiators", Value: "AI-first"},
			"business_model":   {FieldKey: "business_model", Value: "B2B SaaS"},
			"customer_types":   {FieldKey: "customer_types", Value: "Enterprise"},
			"end_markets":      {FieldKey: "end_markets", Value: "Healthcare, Finance"},
		},
	}
	original := &SFReportCompany{
		AccountID: "001ABC",
		Ownership: "Private",
	}

	row := buildSFReportRow(result, original)

	assert.Equal(t, "Acme Corp", row[0])           // Account Name
	assert.Equal(t, "001ABC", row[1])              // Account ID
	assert.Equal(t, "acme.com", row[2])            // Website (stripped scheme)
	assert.Equal(t, "2010", row[3])                // Year Founded
	assert.Equal(t, "3", row[4])                   // Locations
	assert.Equal(t, "250", row[5])                 // Employees
	assert.Equal(t, "IL", row[6])                  // Shipping State
	assert.Equal(t, "Springfield", row[8])         // Shipping City
	assert.Equal(t, "Private", row[13])            // Ownership (from original)
	assert.Contains(t, row[12], "A tech company.") // Research Notes includes description
	assert.Contains(t, row[12], "Services:")
	assert.Contains(t, row[12], "Differentiators:")
	assert.Contains(t, row[12], "Business Model:")
	assert.Contains(t, row[12], "Customers:")
}

// ============================================================
// localcrawl.go — normalizeURL error path
// ============================================================

// TestNormalizeURL_InvalidAfterScheme tests that normalizeURL returns
// an error for a URL that's invalid even after adding a scheme.
func TestNormalizeURL_InvalidURL(t *testing.T) {
	_, err := normalizeURL("://\x00bad")
	assert.Error(t, err)
}

// ============================================================
// export_salesforce.go — Flush error path
// ============================================================

// TestSalesforceExporter_FlushError tests that Flush propagates errors
// from FlushSFWrites.
func TestSalesforceExporter_FlushError(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// BulkUpdateAccounts will fail.
	sfClient.On("UpdateCollection", mock.Anything, "Account", mock.Anything).
		Return(nil, errors.New("bulk update failed"))

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)
	exp.intents = []*SFWriteIntent{
		{
			AccountOp:     "update",
			AccountID:     "001ABC",
			AccountFields: map[string]any{"Industry": "Tech"},
			Result:        &model.EnrichmentResult{Company: model.Company{Name: "Acme"}},
		},
	}

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "flush sf writes")
}

// ============================================================
// searchfilter.go — urlSlug edge case
// ============================================================

// TestURLSlug_EmptyAndSpecial tests urlSlug with edge case inputs.
func TestURLSlug_EmptyAndSpecial(t *testing.T) {
	assert.Equal(t, "", urlSlug(""))
	// URL with no path should return empty slug.
	assert.Equal(t, "", urlSlug("https://acme.com"))
	assert.Equal(t, "", urlSlug("https://acme.com/"))
}

// ============================================================
// scrape.go — resolveGoogleMapsViaAPI with city/state in query
// ============================================================

// TestResolveGoogleMapsViaAPI_WithCityState tests that city and state
// are appended to the query.
func TestResolveGoogleMapsViaAPI_WithCityState(t *testing.T) {
	ctx := context.Background()
	company := model.Company{
		Name:  "Acme Corp",
		City:  "Springfield",
		State: "IL",
	}

	googleClient := googlemocks.NewMockClient(t)
	googleClient.On("TextSearch", mock.Anything, "Acme Corp Springfield IL").
		Return(&google.TextSearchResponse{
			Places: []google.Place{
				{DisplayName: google.DisplayName{Text: "Acme Corp"}, Rating: 4.5, UserRatingCount: 150},
			},
		}, nil)

	meta := resolveGoogleMapsViaAPI(ctx, company, googleClient)
	assert.NotNil(t, meta)
	assert.Equal(t, 4.5, meta.Rating)
	assert.Equal(t, 150, meta.ReviewCount)
	assert.Equal(t, "google_api", meta.Source)
}

// TestResolveGoogleMapsViaAPI_Error tests the error path.
func TestResolveGoogleMapsViaAPI_Error(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp"}

	googleClient := googlemocks.NewMockClient(t)
	googleClient.On("TextSearch", mock.Anything, mock.AnythingOfType("string")).
		Return(nil, errors.New("api error"))

	meta := resolveGoogleMapsViaAPI(ctx, company, googleClient)
	assert.Nil(t, meta)
}

// ============================================================
// scrape.go — resolveBBBViaPerplexity edge cases
// ============================================================

// TestResolveBBBViaPerplexity_EmptyContent tests nil return when
// BBB response content is empty.
func TestResolveBBBViaPerplexity_EmptyContent(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Empty Corp", URL: "https://empty.com"}

	page := resolveBBBViaPerplexity(ctx, company, &mockPerplexityClient{response: ""})
	assert.Nil(t, page)
}

// ============================================================
// gate.go — upsertContacts edge cases
// ============================================================

// TestUpsertContacts_EmptyContacts tests that upsertContacts returns
// zero counts when contacts are empty.
func TestUpsertContacts_EmptyContacts(t *testing.T) {
	ctx := context.Background()
	sfClient := salesforcemocks.NewMockClient(t)

	res := upsertContacts(ctx, sfClient, "001ABC", nil, "Acme")
	assert.Equal(t, 0, res.Created)
	assert.Equal(t, 0, res.Updated)
	assert.Equal(t, 0, res.Failed)
}

// TestUpsertContacts_EmptyAccountID tests that upsertContacts returns
// zero counts when accountID is empty.
func TestUpsertContacts_EmptyAccountID(t *testing.T) {
	ctx := context.Background()
	sfClient := salesforcemocks.NewMockClient(t)

	contacts := []map[string]any{{"FirstName": "John", "LastName": "Doe"}}
	res := upsertContacts(ctx, sfClient, "", contacts, "Acme")
	assert.Equal(t, 0, res.Created)
}

// ============================================================
// score.go — scoreFreshness edge case
// ============================================================

// TestScoreFreshness_AllFieldsMissing tests scoreFreshness when field
// values map is empty but scoreable fields exist (0 contribution).
func TestScoreFreshness_AllFieldsMissing(t *testing.T) {
	fieldValues := map[string]model.FieldValue{}
	scoreable := []model.FieldMapping{
		{Key: "industry", Required: true},
		{Key: "employees"},
	}
	score := scoreFreshness(fieldValues, scoreable, time.Now())
	assert.Equal(t, 0.0, score)
}

// TestScoreFreshness_OldData tests scoreFreshness with data that is
// very old (>3 years), which should return a low score near the floor.
func TestScoreFreshness_OldData(t *testing.T) {
	now := time.Now()
	oldDate := now.Add(-4 * 365 * 24 * time.Hour) // 4 years ago
	fieldValues := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Value: "Tech", DataAsOf: &oldDate},
	}
	scoreable := []model.FieldMapping{
		{Key: "industry"},
	}
	score := scoreFreshness(fieldValues, scoreable, now)
	assert.LessOrEqual(t, score, 0.3) // Should be near the floor (0.2)
}

// ============================================================
// ParsePhoneFromMarkdown edge case
// ============================================================

// TestParsePhoneFromMarkdown_NoPhones tests parsing markdown without
// any phone numbers.
func TestParsePhoneFromMarkdown_NoPhones(t *testing.T) {
	phones := ParsePhoneFromMarkdown("This is a page with no phone numbers at all.")
	assert.Empty(t, phones)
}

// TestParsePhoneFromMarkdown_MultiplePhones tests parsing markdown with
// multiple phone numbers.
func TestParsePhoneFromMarkdown_MultiplePhones(t *testing.T) {
	md := "Call us at (555) 123-4567 or (555) 987-6543."
	phones := ParsePhoneFromMarkdown(md)
	assert.GreaterOrEqual(t, len(phones), 1)
}
