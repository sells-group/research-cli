package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	scrapemocks "github.com/sells-group/research-cli/internal/scrape/mocks"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
	"github.com/sells-group/research-cli/pkg/perplexity"
	perplexitymocks "github.com/sells-group/research-cli/pkg/perplexity/mocks"
)

func TestLinkedInPhase_ChainSuccess(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page: model.CrawledPage{
			Markdown: "Acme Corp is a technology company headquartered in NYC with 200 employees. Founded in 2010. Industry: Technology.",
		},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pplxClient := perplexitymocks.NewMockClient(t) // Should not be called

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{
				Text: `{"company_name": "Acme Corp", "description": "Technology company", "industry": "Technology", "employee_count": "200", "headquarters": "New York City", "founded": "2010", "specialties": "AI, ML", "website": "https://acme.com", "linkedin_url": "", "company_type": "Privately Held", "exec_first_name": "Jane", "exec_last_name": "Doe", "exec_title": "CEO & Founder"}`,
			}},
			Usage: anthropic.TokenUsage{InputTokens: 200, OutputTokens: 100},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, usage, err := LinkedInPhase(ctx, company, chain, pplxClient, aiClient, aiCfg, nil)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, "Acme Corp", data.CompanyName)
	assert.Equal(t, "Technology", data.Industry)
	assert.NotEmpty(t, data.LinkedInURL) // Should be filled in
	assert.Equal(t, "Jane", data.ExecFirstName)
	assert.Equal(t, "Doe", data.ExecLastName)
	assert.Equal(t, "CEO & Founder", data.ExecTitle)
	assert.NotNil(t, usage)
	pplxClient.AssertNotCalled(t, "ChatCompletion") // Perplexity not used
	aiClient.AssertExpectations(t)
}

func TestLinkedInPhase_ChainLoginWall_FallsBackToPerplexity(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	s := scrapemocks.NewMockScraper(t)
	s.On("Name").Return("mock").Maybe()
	s.On("Supports", mock.Anything).Return(true).Maybe()
	s.On("Scrape", mock.Anything, mock.Anything).Return(&scrape.Result{
		Page:   model.CrawledPage{Markdown: "Sign in to view this profile. Join now to see full content."},
		Source: "mock",
	}, nil).Maybe()
	chain := scrape.NewChain(scrape.NewPathMatcher(nil), s)

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Acme Corp is a tech company."}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{
				Text: `{"company_name": "Acme Corp", "description": "Tech company", "industry": "Technology", "employee_count": "", "headquarters": "", "founded": "", "specialties": "", "website": "", "linkedin_url": "", "company_type": ""}`,
			}},
			Usage: anthropic.TokenUsage{InputTokens: 200, OutputTokens: 100},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, usage, err := LinkedInPhase(ctx, company, chain, pplxClient, aiClient, aiCfg, nil)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, "Acme Corp", data.CompanyName)
	assert.NotNil(t, usage)
	assert.Equal(t, 300, usage.InputTokens) // 100 from pplx + 200 from ai
	pplxClient.AssertExpectations(t)
	aiClient.AssertExpectations(t)
}

func TestLinkedInPhase_NilChain_FallsBackToPerplexity(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Acme Corp is a technology company headquartered in NYC with 200 employees."}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{
				Text: `{"company_name": "Acme Corp", "description": "Technology company", "industry": "Technology", "employee_count": "200", "headquarters": "New York City", "founded": "2010", "specialties": "AI, ML", "website": "https://acme.com", "linkedin_url": "https://linkedin.com/company/acme", "company_type": "Privately Held"}`,
			}},
			Usage: anthropic.TokenUsage{InputTokens: 200, OutputTokens: 100},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, usage, err := LinkedInPhase(ctx, company, nil, pplxClient, aiClient, aiCfg, nil)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, "Acme Corp", data.CompanyName)
	assert.NotNil(t, usage)
	assert.Equal(t, 300, usage.InputTokens)
	pplxClient.AssertExpectations(t)
	aiClient.AssertExpectations(t)
}

func TestLinkedInPhase_PerplexityError(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(nil, errors.New("api error"))

	aiClient := anthropicmocks.NewMockClient(t)
	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	// nil chain → falls to perplexity which errors
	data, _, err := LinkedInPhase(ctx, company, nil, pplxClient, aiClient, aiCfg, nil)

	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "perplexity search")
}

func TestLinkedInPhase_HaikuParseError(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Some LinkedIn data."}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{Text: "Not valid JSON at all"}},
			Usage:   anthropic.TokenUsage{InputTokens: 200, OutputTokens: 50},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, usage, err := LinkedInPhase(ctx, company, nil, pplxClient, aiClient, aiCfg, nil)

	assert.Error(t, err)
	assert.Nil(t, data)
	assert.NotNil(t, usage)
	assert.Contains(t, err.Error(), "parse haiku json")
}

func TestLinkedInPhase_EmptyName_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	company := model.Company{URL: "https://acme.com"} // No Name.

	data, usage, err := LinkedInPhase(ctx, company, nil, nil, nil, config.AnthropicConfig{}, nil)

	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.NotNil(t, usage)
	assert.Equal(t, 0, usage.InputTokens)
}

func TestBuildLinkedInURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want string
	}{
		{"Acme Corp", "https://www.linkedin.com/company/acme"},
		{"Smith & Sons LLC", "https://www.linkedin.com/company/smith-and-sons"},
		{"Acme Industrial", "https://www.linkedin.com/company/acme-industrial"},
		{"Simple", "https://www.linkedin.com/company/simple"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, buildLinkedInURL(tt.name))
		})
	}
}

func TestLinkedInPhase_ExecContacts(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Acme Corp, CEO Jane Doe, VP John Smith, Director Bob Jones"}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{
				Text: `{"company_name": "Acme Corp", "description": "Tech", "industry": "Technology", "employee_count": "200", "headquarters": "NYC", "founded": "2010", "specialties": "", "website": "https://acme.com", "linkedin_url": "", "company_type": "Privately Held", "exec_first_name": "", "exec_last_name": "", "exec_title": "", "exec_contacts": [{"first_name": "Jane", "last_name": "Doe", "title": "CEO", "linkedin_url": "https://linkedin.com/in/janedoe"}, {"first_name": "John", "last_name": "Smith", "title": "VP Operations"}, {"first_name": "Bob", "last_name": "Jones", "title": "Director"}]}`,
			}},
			Usage: anthropic.TokenUsage{InputTokens: 200, OutputTokens: 100},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, _, err := LinkedInPhase(ctx, company, nil, pplxClient, aiClient, aiCfg, nil)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Len(t, data.ExecContacts, 3)
	assert.Equal(t, "Jane", data.ExecContacts[0].FirstName)
	assert.Equal(t, "Doe", data.ExecContacts[0].LastName)
	assert.Equal(t, "CEO", data.ExecContacts[0].Title)
	assert.Equal(t, "https://linkedin.com/in/janedoe", data.ExecContacts[0].LinkedInURL)
	assert.Equal(t, "John", data.ExecContacts[1].FirstName)
	assert.Equal(t, "Bob", data.ExecContacts[2].FirstName)

	// Backward compat: flat exec fields auto-populated from ExecContacts[0].
	assert.Equal(t, "Jane", data.ExecFirstName)
	assert.Equal(t, "Doe", data.ExecLastName)
	assert.Equal(t, "CEO", data.ExecTitle)
}

func TestLinkedInPhase_ExecContacts_BackwardCompat(t *testing.T) {
	// When Haiku returns both flat exec fields and ExecContacts, flat fields take priority.
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Acme Corp data"}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 50},
		}, nil)

	aiClient := anthropicmocks.NewMockClient(t)
	aiClient.On("CreateMessage", ctx, mock.AnythingOfType("anthropic.MessageRequest")).
		Return(&anthropic.MessageResponse{
			Content: []anthropic.ContentBlock{{
				Text: `{"company_name": "Acme Corp", "description": "", "industry": "", "employee_count": "", "headquarters": "", "founded": "", "specialties": "", "website": "", "linkedin_url": "", "company_type": "", "exec_first_name": "Alice", "exec_last_name": "Wonder", "exec_title": "President", "exec_contacts": [{"first_name": "Jane", "last_name": "Doe", "title": "CEO"}]}`,
			}},
			Usage: anthropic.TokenUsage{InputTokens: 200, OutputTokens: 100},
		}, nil)

	aiCfg := config.AnthropicConfig{HaikuModel: "claude-haiku-4-5-20251001"}

	data, _, err := LinkedInPhase(ctx, company, nil, pplxClient, aiClient, aiCfg, nil)

	assert.NoError(t, err)
	// Flat fields should NOT be overridden since they're already populated.
	assert.Equal(t, "Alice", data.ExecFirstName)
	assert.Equal(t, "Wonder", data.ExecLastName)
	assert.Equal(t, "President", data.ExecTitle)
}

func TestIsLinkedInLoginWall(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{"short content", "short", true},
		{"sign in page", "Please Sign in to continue viewing this profile on LinkedIn. Join now for free.", true},
		{"authwall", "https://www.linkedin.com/authwall?redirect=...", true},
		{"real content", "Acme Corp is a leading technology company founded in 2010. We provide innovative solutions for enterprise customers worldwide. Our team of 200+ engineers builds cutting-edge products.", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isLinkedInLoginWall(tt.content))
		})
	}
}
