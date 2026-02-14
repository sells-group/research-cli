package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
	firecrawlmocks "github.com/sells-group/research-cli/pkg/firecrawl/mocks"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	"github.com/sells-group/research-cli/pkg/perplexity"
	perplexitymocks "github.com/sells-group/research-cli/pkg/perplexity/mocks"
	pppmocks "github.com/sells-group/research-cli/pkg/ppp/mocks"
	salesforcemocks "github.com/sells-group/research-cli/pkg/salesforce/mocks"
)

func TestPipeline_Run_FullFlow(t *testing.T) {
	ctx := context.Background()

	company := model.Company{
		URL:          "https://acme.com",
		Name:         "Acme Corp",
		SalesforceID: "001ABC",
		NotionPageID: "page-123",
	}

	questions := []model.Question{
		{ID: "q1", Text: "What industry?", Tier: 1, FieldKey: "industry", PageTypes: []model.PageType{model.PageTypeAbout}, OutputFormat: "string"},
		{ID: "q2", Text: "How many employees?", Tier: 1, FieldKey: "employees", PageTypes: []model.PageType{model.PageTypeAbout}, OutputFormat: "number"},
	}

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", DataType: "string", Required: true},
		{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"},
	})

	cfg := &config.Config{
		Crawl: config.CrawlConfig{
			MaxPages:      50,
			MaxDepth:      2,
			CacheTTLHours: 24,
		},
		Pipeline: config.PipelineConfig{
			ConfidenceEscalationThreshold: 0.4,
			Tier3Gate:                     "ambiguity_only",
			QualityScoreThreshold:         0.5,
		},
		Anthropic: config.AnthropicConfig{
			HaikuModel:  "claude-haiku-4-5-20251001",
			SonnetModel: "claude-sonnet-4-5-20250929",
			OpusModel:   "claude-opus-4-6",
		},
	}

	// --- Set up mocks ---
	// Use mock.Anything for context since errgroup wraps it in a cancelCtx.

	st := storemocks.NewMockStore(t)
	st.On("CreateRun", mock.Anything, company).Return(&model.Run{
		ID:      "run-001",
		Company: company,
		Status:  model.RunStatusQueued,
	}, nil)
	st.On("UpdateRunStatus", mock.Anything, "run-001", mock.AnythingOfType("model.RunStatus")).Return(nil)
	st.On("CreatePhase", mock.Anything, "run-001", mock.AnythingOfType("string")).Return(&model.RunPhase{ID: "phase-001"}, nil)
	st.On("CompletePhase", mock.Anything, "phase-001", mock.AnythingOfType("*model.PhaseResult")).Return(nil)
	st.On("GetCachedCrawl", mock.Anything, "https://acme.com").Return(&model.CrawlCache{
		CompanyURL: "https://acme.com",
		Pages: []model.CrawledPage{
			{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme Corporation, a technology company."},
			{URL: "https://acme.com/about", Title: "About Acme", Markdown: "Acme Corp has 200 employees and operates in the technology industry."},
		},
		CrawledAt: time.Now(),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}, nil)
	st.On("UpdateRunResult", mock.Anything, "run-001", mock.AnythingOfType("*model.RunResult")).Return(nil)

	// Scrape chain — for scrape phase (external sources) and LinkedIn.
	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			URL:      "https://example.com",
			Title:    "External Source",
			Markdown: "Acme Corp information from external source with details about their operations and industry presence in the tech sector.",
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	fcClient := firecrawlmocks.NewMockClient(t)

	// Perplexity mock for LinkedIn phase.
	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", mock.Anything, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Acme Corp LinkedIn: Technology company, 200 employees, NYC."}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil).Maybe()

	// Anthropic mock - used by LinkedIn (Haiku JSON), classify, and extract.
	aiClient := anthropicmocks.NewMockClient(t)

	// CreateMessage: generic response for all direct calls (LinkedIn, classification direct, extraction).
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9, "value": "Technology", "company_name": "Acme Corp", "description": "Tech company", "industry": "Technology", "employee_count": "200", "headquarters": "NYC", "founded": "2010", "specialties": "Tech", "website": "https://acme.com", "linkedin_url": "https://linkedin.com/company/acme", "company_type": "Private", "reasoning": "from page", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil)

	// CreateBatch: for classification (7 pages > 3 threshold) and extraction batches.
	aiClient.On("CreateBatch", mock.Anything, mock.AnythingOfType("anthropic.BatchRequest")).
		Return(&anthropic.BatchResponse{
			ID:               "batch-001",
			ProcessingStatus: "ended",
		}, nil).Maybe()

	// GetBatch: poll returns ended immediately.
	aiClient.On("GetBatch", mock.Anything, "batch-001").
		Return(&anthropic.BatchResponse{
			ID:               "batch-001",
			ProcessingStatus: "ended",
		}, nil).Maybe()

	// GetBatchResults: return results for all batch items.
	batchResults := []anthropic.BatchResultItem{}
	for i := 0; i < 20; i++ { // enough for any batch size
		batchResults = append(batchResults, anthropic.BatchResultItem{
			CustomID: "",
			Type:     "succeeded",
			Message: &anthropic.MessageResponse{
				Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9, "value": "Technology", "reasoning": "from page", "source_url": "https://acme.com/about"}`}},
				Usage:   anthropic.TokenUsage{InputTokens: 50, OutputTokens: 10},
			},
		})
	}
	aiClient.On("GetBatchResults", mock.Anything, "batch-001").
		Return(setupBatchIterator(t, batchResults), nil).Maybe()

	// Salesforce mock.
	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).Return(nil).Maybe()

	// Notion mock.
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil).Maybe()

	// PPP mock.
	pppClient := pppmocks.NewMockQuerier(t)
	pppClient.On("FindLoans", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).
		Return(nil, nil).Maybe()

	// --- Run pipeline ---
	p := New(cfg, st, chain, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, questions, fields)

	result, err := p.Run(ctx, company)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "run-001", result.RunID)
	assert.Equal(t, "Acme Corp", result.Company.Name)
	assert.NotEmpty(t, result.Report)
	assert.NotEmpty(t, result.Phases)

	// Verify phases were executed.
	phaseNames := make(map[string]bool)
	for _, p := range result.Phases {
		phaseNames[p.Name] = true
	}
	assert.True(t, phaseNames["1a_crawl"])
	assert.True(t, phaseNames["1b_scrape"])
	assert.True(t, phaseNames["1c_linkedin"])
	assert.True(t, phaseNames["1d_ppp"])
	assert.True(t, phaseNames["2_classify"])
	assert.True(t, phaseNames["3_route"])
	assert.True(t, phaseNames["7_aggregate"])
	assert.True(t, phaseNames["8_report"])
	assert.True(t, phaseNames["9_gate"])

	st.AssertExpectations(t)
}

func TestPipeline_New(t *testing.T) {
	cfg := &config.Config{}
	st := storemocks.NewMockStore(t)
	chain := scrape.NewChain(scrape.NewPathMatcher(nil))
	fcClient := firecrawlmocks.NewMockClient(t)
	pplxClient := perplexitymocks.NewMockClient(t)
	aiClient := anthropicmocks.NewMockClient(t)
	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	pppClient := pppmocks.NewMockQuerier(t)

	questions := []model.Question{{ID: "q1"}}
	fields := model.NewFieldRegistry(nil)

	p := New(cfg, st, chain, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, questions, fields)

	assert.NotNil(t, p)
	assert.Equal(t, cfg, p.cfg)
	assert.Len(t, p.questions, 1)
}

func TestLinkedInToPage(t *testing.T) {
	data := &LinkedInData{
		CompanyName:   "Acme Corp",
		Description:   "Technology company",
		Industry:      "Technology",
		EmployeeCount: "200",
		Headquarters:  "NYC",
		LinkedInURL:   "https://linkedin.com/company/acme",
	}
	company := model.Company{Name: "Acme Corp"}

	page := linkedInToPage(data, company)

	assert.Equal(t, "https://linkedin.com/company/acme", page.URL)
	assert.Contains(t, page.Title, "linkedin")
	assert.Contains(t, page.Markdown, "Acme Corp")
	assert.Contains(t, page.Markdown, "Technology")
	assert.Contains(t, page.Markdown, "200")
	assert.Equal(t, 200, page.StatusCode)
}
