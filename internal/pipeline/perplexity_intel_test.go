package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/perplexity"
	perplexitymocks "github.com/sells-group/research-cli/pkg/perplexity/mocks"
)

func TestPerplexityIntelPhase_Success(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Med Repair Tech", URL: "https://medrepairtech.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "Med Repair Tech was founded in 2015. They have approximately 50 employees. Headquarters: Dallas, TX."}},
			},
			Usage: perplexity.Usage{PromptTokens: 120, CompletionTokens: 80},
		}, nil)

	page, usage, err := PerplexityIntelPhase(ctx, company, pplxClient)

	assert.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, "perplexity://intel/medrepairtech.com", page.URL)
	assert.Equal(t, "[perplexity_intel] Med Repair Tech", page.Title)
	assert.Contains(t, page.Markdown, "founded in 2015")
	assert.Contains(t, page.Markdown, "50 employees")
	assert.Equal(t, 200, page.StatusCode)
	assert.NotNil(t, usage)
	assert.Equal(t, 120, usage.InputTokens)
	assert.Equal(t, 80, usage.OutputTokens)
}

func TestPerplexityIntelPhase_NilClient(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	page, usage, err := PerplexityIntelPhase(ctx, company, nil)

	assert.NoError(t, err)
	assert.Nil(t, page)
	assert.NotNil(t, usage)
	assert.Equal(t, 0, usage.InputTokens)
}

func TestPerplexityIntelPhase_Error(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(nil, errors.New("api error"))

	page, usage, err := PerplexityIntelPhase(ctx, company, pplxClient)

	assert.Error(t, err)
	assert.Nil(t, page)
	assert.Contains(t, err.Error(), "perplexity_intel: query failed")
	assert.NotNil(t, usage)
}

func TestPerplexityIntelPhase_EmptyResponse(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{
				{Message: perplexity.Message{Content: "   "}},
			},
			Usage: perplexity.Usage{PromptTokens: 100, CompletionTokens: 5},
		}, nil)

	page, usage, err := PerplexityIntelPhase(ctx, company, pplxClient)

	assert.NoError(t, err)
	assert.Nil(t, page)
	assert.NotNil(t, usage)
}

func TestPerplexityIntelPhase_NoChoices(t *testing.T) {
	ctx := context.Background()
	company := model.Company{Name: "Acme Corp", URL: "https://acme.com"}

	pplxClient := perplexitymocks.NewMockClient(t)
	pplxClient.On("ChatCompletion", ctx, mock.AnythingOfType("perplexity.ChatCompletionRequest")).
		Return(&perplexity.ChatCompletionResponse{
			Choices: []perplexity.Choice{},
			Usage:   perplexity.Usage{PromptTokens: 100, CompletionTokens: 0},
		}, nil)

	page, usage, err := PerplexityIntelPhase(ctx, company, pplxClient)

	assert.NoError(t, err)
	assert.Nil(t, page)
	assert.NotNil(t, usage)
}

func TestParsePerplexityIntel_YearAndEmployees(t *testing.T) {
	markdown := "Med Repair Tech was founded in 2015. They have approximately 50 employees and specialize in medical equipment repair."

	results := ParsePerplexityIntel(markdown, nil)

	assert.Len(t, results, 2)

	byField := make(map[string]model.ExtractionAnswer)
	for _, a := range results {
		byField[a.FieldKey] = a
	}

	yr := byField["year_founded"]
	assert.Equal(t, 2015, yr.Value)
	assert.Equal(t, 0.60, yr.Confidence)
	assert.Equal(t, "perplexity_intel", yr.Source)

	emp := byField["employee_count"]
	assert.Equal(t, 50, emp.Value)
	assert.Equal(t, 0.55, emp.Confidence)
	assert.Equal(t, "perplexity_intel", emp.Source)
}

func TestParsePerplexityIntel_EmployeeRange(t *testing.T) {
	markdown := "The company employs 100-200 employees and was established in 2008."

	results := ParsePerplexityIntel(markdown, nil)

	assert.Len(t, results, 2)

	byField := make(map[string]model.ExtractionAnswer)
	for _, a := range results {
		byField[a.FieldKey] = a
	}

	emp := byField["employee_count"]
	assert.Equal(t, 150, emp.Value) // midpoint of 100-200

	yr := byField["year_founded"]
	assert.Equal(t, 2008, yr.Value)
}

func TestParsePerplexityIntel_SkipsExisting(t *testing.T) {
	markdown := "Founded in 2015. Has about 50 employees."
	existing := []model.ExtractionAnswer{
		{FieldKey: "year_founded", Value: 2010, Confidence: 0.8},
		{FieldKey: "employee_count", Value: 100, Confidence: 0.7},
	}

	results := ParsePerplexityIntel(markdown, existing)
	assert.Len(t, results, 0) // Both fields already populated.
}

func TestParsePerplexityIntel_PartialSkip(t *testing.T) {
	markdown := "Founded in 2015. Has about 50 employees."
	existing := []model.ExtractionAnswer{
		{FieldKey: "year_founded", Value: 2010, Confidence: 0.8},
	}

	results := ParsePerplexityIntel(markdown, existing)
	assert.Len(t, results, 1)
	assert.Equal(t, "employee_count", results[0].FieldKey)
	assert.Equal(t, 50, results[0].Value)
}

func TestParsePerplexityIntel_EmptyMarkdown(t *testing.T) {
	results := ParsePerplexityIntel("", nil)
	assert.Nil(t, results)
}

func TestParsePerplexityIntel_NoMatches(t *testing.T) {
	markdown := "This company provides excellent medical equipment services in the Dallas area."
	results := ParsePerplexityIntel(markdown, nil)
	assert.Len(t, results, 0)
}

func TestParsePerplexityIntel_CommaEmployees(t *testing.T) {
	markdown := "The firm has approximately 1,500 employees worldwide."
	results := ParsePerplexityIntel(markdown, nil)
	assert.Len(t, results, 1)
	assert.Equal(t, "employee_count", results[0].FieldKey)
	assert.Equal(t, 1500, results[0].Value)
}

func TestParseCommaInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"1500", 1500},
		{"1,500", 1500},
		{"50", 50},
		{"10,001", 10001},
		{"bad", 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, parseCommaInt(tt.input))
		})
	}
}

func TestExtractIntelDomain(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://www.acme.com", "acme.com"},
		{"https://acme.com/about", "acme.com"},
		{"https://medrepairtech.com", "medrepairtech.com"},
		{"not-a-url", "not-a-url"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.want, extractIntelDomain(tt.url))
		})
	}
}
