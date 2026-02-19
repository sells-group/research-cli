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
	"github.com/sells-group/research-cli/pkg/jina"
	jinamocks "github.com/sells-group/research-cli/pkg/jina/mocks"
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
	st.On("GetCachedLinkedIn", mock.Anything, "acme.com").Return(nil, nil)
	st.On("SetCachedLinkedIn", mock.Anything, "acme.com", mock.Anything, mock.Anything).Return(nil).Maybe()
	st.On("GetHighConfidenceAnswers", mock.Anything, "https://acme.com", mock.AnythingOfType("float64")).Return(nil, nil)
	st.On("LoadCheckpoint", mock.Anything, "https://acme.com").Return(nil, nil)
	st.On("SaveCheckpoint", mock.Anything, "https://acme.com", mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	st.On("DeleteCheckpoint", mock.Anything, "https://acme.com").Return(nil)

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

	// Jina mock for search-then-scrape.
	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{
			Code: 200,
			Data: []jina.SearchResult{
				{Title: "External", URL: "https://example.com/profile", Content: "content"},
			},
		}, nil).Maybe()

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

	// Batch API mocks not needed: external pages are auto-classified,
	// leaving <=3 pages for LLM classify (direct mode), and <=3 questions
	// for extraction (direct mode). No batch path is hit.

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
	p := New(cfg, st, chain, jinaClient, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, nil, nil, questions, fields)

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
	jinaClient := jinamocks.NewMockClient(t)
	fcClient := firecrawlmocks.NewMockClient(t)
	pplxClient := perplexitymocks.NewMockClient(t)
	aiClient := anthropicmocks.NewMockClient(t)
	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	pppClient := pppmocks.NewMockQuerier(t)

	questions := []model.Question{{ID: "q1"}}
	fields := model.NewFieldRegistry(nil)

	p := New(cfg, st, chain, jinaClient, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, nil, nil, questions, fields)

	assert.NotNil(t, p)
	assert.Equal(t, cfg, p.cfg)
	assert.Len(t, p.questions, 1)
}

// --- filterRoutedQuestions unit tests ---

func TestFilterRoutedQuestions(t *testing.T) {
	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "industry"}},
		{Question: model.Question{ID: "q2", FieldKey: "revenue"}},
		{Question: model.Question{ID: "q3", FieldKey: "employees"}},
	}
	existingKeys := map[string]bool{
		"revenue": true,
	}
	var skipped int
	filtered := filterRoutedQuestions(routed, existingKeys, &skipped)
	assert.Len(t, filtered, 2)
	assert.Equal(t, 1, skipped)
	// Verify "revenue" was removed
	for _, rq := range filtered {
		assert.NotEqual(t, "revenue", rq.Question.FieldKey)
	}
}

func TestFilterRoutedQuestions_Empty(t *testing.T) {
	var skipped int
	filtered := filterRoutedQuestions(nil, map[string]bool{"a": true}, &skipped)
	assert.Empty(t, filtered)
	assert.Equal(t, 0, skipped)
}

func TestFilterRoutedQuestions_NoExisting(t *testing.T) {
	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "industry"}},
	}
	var skipped int
	filtered := filterRoutedQuestions(routed, map[string]bool{}, &skipped)
	assert.Len(t, filtered, 1)
	assert.Equal(t, 0, skipped)
}

func TestFilterRoutedQuestions_AllExisting(t *testing.T) {
	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "industry"}},
		{Question: model.Question{ID: "q2", FieldKey: "revenue"}},
	}
	existingKeys := map[string]bool{
		"industry": true,
		"revenue":  true,
	}
	var skipped int
	filtered := filterRoutedQuestions(routed, existingKeys, &skipped)
	assert.Empty(t, filtered)
	assert.Equal(t, 2, skipped)
}

// --- Pipeline integration tests ---

func TestPipeline_ExistingAnswerLookup_SkipsQuestions(t *testing.T) {
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
			SkipConfidenceThreshold:       0.8,
		},
		Anthropic: config.AnthropicConfig{
			HaikuModel:  "claude-haiku-4-5-20251001",
			SonnetModel: "claude-sonnet-4-5-20250929",
			OpusModel:   "claude-opus-4-6",
		},
	}

	st := storemocks.NewMockStore(t)
	st.On("CreateRun", mock.Anything, company).Return(&model.Run{
		ID:      "run-002",
		Company: company,
		Status:  model.RunStatusQueued,
	}, nil)
	st.On("UpdateRunStatus", mock.Anything, "run-002", mock.AnythingOfType("model.RunStatus")).Return(nil)
	st.On("CreatePhase", mock.Anything, "run-002", mock.AnythingOfType("string")).Return(&model.RunPhase{ID: "phase-002"}, nil)
	st.On("CompletePhase", mock.Anything, "phase-002", mock.AnythingOfType("*model.PhaseResult")).Return(nil)
	st.On("GetCachedCrawl", mock.Anything, "https://acme.com").Return(&model.CrawlCache{
		CompanyURL: "https://acme.com",
		Pages: []model.CrawledPage{
			{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme."},
			{URL: "https://acme.com/about", Title: "About", Markdown: "Acme Corp has 200 employees in technology."},
		},
		CrawledAt: time.Now(),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}, nil)
	st.On("UpdateRunResult", mock.Anything, "run-002", mock.AnythingOfType("*model.RunResult")).Return(nil)
	st.On("GetCachedLinkedIn", mock.Anything, "acme.com").Return(nil, nil)
	st.On("SetCachedLinkedIn", mock.Anything, "acme.com", mock.Anything, mock.Anything).Return(nil).Maybe()
	// Return existing high-confidence answer for "industry" — should skip that question.
	st.On("GetHighConfidenceAnswers", mock.Anything, "https://acme.com", mock.AnythingOfType("float64")).Return([]model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Technology", Confidence: 0.95, Tier: 1},
	}, nil)
	st.On("LoadCheckpoint", mock.Anything, "https://acme.com").Return(nil, nil)
	st.On("SaveCheckpoint", mock.Anything, "https://acme.com", mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	st.On("DeleteCheckpoint", mock.Anything, "https://acme.com").Return(nil)

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page:   model.CrawledPage{URL: "https://example.com", Title: "External", Markdown: "Acme Corp info."},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	fcClient := firecrawlmocks.NewMockClient(t)

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{Code: 200, Data: []jina.SearchResult{{Title: "Ext", URL: "https://example.com/p", Content: "c"}}}, nil).Maybe()

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", mock.Anything, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{{Message: perplexity.Message{Content: "Acme Corp LinkedIn: Tech, 200 employees."}}},
			Usage:   perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil).Maybe()

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9, "value": "200", "company_name": "Acme Corp", "description": "Tech", "industry": "Technology", "employee_count": "200", "headquarters": "NYC", "founded": "2010", "specialties": "Tech", "website": "https://acme.com", "linkedin_url": "https://linkedin.com/company/acme", "company_type": "Private", "reasoning": "from page", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil)

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).Return(nil).Maybe()

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil).Maybe()

	pppClient := pppmocks.NewMockQuerier(t)
	pppClient.On("FindLoans", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil, nil).Maybe()

	p := New(cfg, st, chain, jinaClient, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, nil, nil, questions, fields)

	result, err := p.Run(ctx, company)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	// The aggregate phase should reflect that existing answers were reused.
	for _, ph := range result.Phases {
		if ph.Name == "7_aggregate" {
			if meta, ok := ph.Metadata["reused_from_existing"]; ok {
				assert.Equal(t, 1, meta)
			}
		}
	}

	st.AssertExpectations(t)
}

func TestPipeline_Checkpoint_ResumesFromT1(t *testing.T) {
	ctx := context.Background()

	company := model.Company{
		URL:          "https://acme.com",
		Name:         "Acme Corp",
		SalesforceID: "001ABC",
		NotionPageID: "page-123",
	}

	questions := []model.Question{
		{ID: "q1", Text: "What industry?", Tier: 1, FieldKey: "industry", PageTypes: []model.PageType{model.PageTypeAbout}, OutputFormat: "string"},
	}

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", DataType: "string", Required: true},
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

	checkpointData := `[{"question_id":"q1","field_key":"industry","value":"Technology","confidence":0.9,"source":"about","source_url":"https://acme.com/about","tier":1,"reasoning":"from about page"}]`

	st := storemocks.NewMockStore(t)
	st.On("CreateRun", mock.Anything, company).Return(&model.Run{
		ID:      "run-003",
		Company: company,
		Status:  model.RunStatusQueued,
	}, nil)
	st.On("UpdateRunStatus", mock.Anything, "run-003", mock.AnythingOfType("model.RunStatus")).Return(nil)
	st.On("CreatePhase", mock.Anything, "run-003", mock.AnythingOfType("string")).Return(&model.RunPhase{ID: "phase-003"}, nil)
	st.On("CompletePhase", mock.Anything, "phase-003", mock.AnythingOfType("*model.PhaseResult")).Return(nil)
	st.On("GetCachedCrawl", mock.Anything, "https://acme.com").Return(&model.CrawlCache{
		CompanyURL: "https://acme.com",
		Pages: []model.CrawledPage{
			{URL: "https://acme.com", Title: "Home", Markdown: "Welcome to Acme."},
			{URL: "https://acme.com/about", Title: "About", Markdown: "Acme Corp in technology."},
		},
		CrawledAt: time.Now(),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}, nil)
	st.On("UpdateRunResult", mock.Anything, "run-003", mock.AnythingOfType("*model.RunResult")).Return(nil)
	st.On("GetCachedLinkedIn", mock.Anything, "acme.com").Return(nil, nil)
	st.On("SetCachedLinkedIn", mock.Anything, "acme.com", mock.Anything, mock.Anything).Return(nil).Maybe()
	st.On("GetHighConfidenceAnswers", mock.Anything, "https://acme.com", mock.AnythingOfType("float64")).Return(nil, nil)
	// Return a T1 checkpoint — pipeline should skip T1 extraction and use these answers.
	st.On("LoadCheckpoint", mock.Anything, "https://acme.com").Return(&model.Checkpoint{
		CompanyID: "https://acme.com",
		Phase:     "t1_complete",
		Data:      []byte(checkpointData),
	}, nil)
	st.On("SaveCheckpoint", mock.Anything, "https://acme.com", mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	st.On("DeleteCheckpoint", mock.Anything, "https://acme.com").Return(nil)

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page:   model.CrawledPage{URL: "https://example.com", Title: "External", Markdown: "Acme info."},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	fcClient := firecrawlmocks.NewMockClient(t)

	jinaClient := jinamocks.NewMockClient(t)
	jinaClient.On("Search", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(&jina.SearchResponse{Code: 200, Data: []jina.SearchResult{{Title: "Ext", URL: "https://example.com/p", Content: "c"}}}, nil).Maybe()

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", mock.Anything, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{{Message: perplexity.Message{Content: "Acme LinkedIn info."}}},
			Usage:   perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil).Maybe()

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", mock.Anything, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: `{"page_type": "about", "confidence": 0.9, "value": "Technology", "company_name": "Acme Corp", "description": "Tech", "industry": "Technology", "employee_count": "200", "headquarters": "NYC", "founded": "2010", "specialties": "Tech", "website": "https://acme.com", "linkedin_url": "https://linkedin.com/company/acme", "company_type": "Private", "reasoning": "from page", "source_url": "https://acme.com/about"}`}},
			Usage:   anthropic.TokenUsage{InputTokens: 100, OutputTokens: 20},
		}, nil)

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).Return(nil).Maybe()

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil).Maybe()

	pppClient := pppmocks.NewMockQuerier(t)
	pppClient.On("FindLoans", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil, nil).Maybe()

	p := New(cfg, st, chain, jinaClient, fcClient, pplxClient, aiClient, sfClient, notionClient, pppClient, nil, nil, questions, fields)

	result, err := p.Run(ctx, company)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify T1 phase used checkpoint (from_checkpoint metadata).
	for _, ph := range result.Phases {
		if ph.Name == "4_extract_t1" {
			if meta, ok := ph.Metadata["from_checkpoint"]; ok {
				assert.Equal(t, true, meta)
			}
		}
	}

	st.AssertExpectations(t)
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
