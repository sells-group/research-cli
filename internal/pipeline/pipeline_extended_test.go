package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
)

func TestConvertAnthropicPricing_Nil(t *testing.T) {
	result := convertAnthropicPricing(nil)
	assert.Nil(t, result)
}

func TestConvertAnthropicPricing_Empty(t *testing.T) {
	result := convertAnthropicPricing(map[string]config.ModelPricing{})
	assert.Nil(t, result)
}

func TestConvertAnthropicPricing_WithModels(t *testing.T) {
	src := map[string]config.ModelPricing{
		"haiku": {
			Input:         0.25,
			Output:        1.25,
			BatchDiscount: 0.5,
			CacheWriteMul: 1.25,
			CacheReadMul:  0.1,
		},
		"sonnet": {
			Input:         3.0,
			Output:        15.0,
			BatchDiscount: 0.5,
			CacheWriteMul: 1.25,
			CacheReadMul:  0.1,
		},
	}

	result := convertAnthropicPricing(src)

	assert.NotNil(t, result)
	assert.Len(t, result, 2)
	assert.Equal(t, 0.25, result["haiku"].Input)
	assert.Equal(t, 1.25, result["haiku"].Output)
	assert.Equal(t, 0.5, result["haiku"].BatchDiscount)
	assert.Equal(t, 1.25, result["haiku"].CacheWriteMul)
	assert.Equal(t, 0.1, result["haiku"].CacheReadMul)
	assert.Equal(t, 3.0, result["sonnet"].Input)
	assert.Equal(t, 15.0, result["sonnet"].Output)
}

func TestLinkedInToPage_AllFields(t *testing.T) {
	data := &LinkedInData{
		CompanyName:   "Test Corp",
		Description:   "A test company",
		Industry:      "Technology",
		EmployeeCount: "500-1000",
		Headquarters:  "San Francisco, CA",
		Founded:       "2015",
		Specialties:   "AI, ML, Data",
		Website:       "https://test.com",
		LinkedInURL:   "https://linkedin.com/company/test-corp",
		CompanyType:   "Privately Held",
	}
	company := model.Company{Name: "Test Corp"}

	page := linkedInToPage(data, company)

	assert.Equal(t, "https://linkedin.com/company/test-corp", page.URL)
	assert.Contains(t, page.Title, "linkedin")
	assert.Contains(t, page.Title, "Test Corp")
	assert.Contains(t, page.Markdown, "**Company Name:** Test Corp")
	assert.Contains(t, page.Markdown, "**Description:** A test company")
	assert.Contains(t, page.Markdown, "**Industry:** Technology")
	assert.Contains(t, page.Markdown, "**Employee Count:** 500-1000")
	assert.Contains(t, page.Markdown, "**Headquarters:** San Francisco, CA")
	assert.Contains(t, page.Markdown, "**Founded:** 2015")
	assert.Contains(t, page.Markdown, "**Specialties:** AI, ML, Data")
	assert.Contains(t, page.Markdown, "**Website:** https://test.com")
	assert.Contains(t, page.Markdown, "**Company Type:** Privately Held")
	assert.Equal(t, 200, page.StatusCode)
}

func TestLinkedInToPage_EmptyFields(t *testing.T) {
	data := &LinkedInData{
		CompanyName: "Empty Corp",
		LinkedInURL: "https://linkedin.com/company/empty",
	}
	company := model.Company{Name: "Empty Corp"}

	page := linkedInToPage(data, company)

	assert.Contains(t, page.Markdown, "**Company Name:** Empty Corp")
	assert.NotContains(t, page.Markdown, "**Description:**")
	assert.NotContains(t, page.Markdown, "**Industry:**")
	assert.NotContains(t, page.Markdown, "**Employee Count:**")
}

func TestComputePhaseCost_AllPhases(t *testing.T) {
	cfg := &config.Config{
		Anthropic: config.AnthropicConfig{
			HaikuModel:  "haiku-test",
			SonnetModel: "sonnet-test",
			OpusModel:   "opus-test",
		},
	}

	p := &Pipeline{
		cfg:      cfg,
		costCalc: nil, // costCalc is nil but we test via the mapping
	}

	// Test that unknown phases return 0.
	cost := p.computePhaseCost("7_aggregate", model.TokenUsage{InputTokens: 100})
	assert.Equal(t, 0.0, cost)

	cost = p.computePhaseCost("8_report", model.TokenUsage{InputTokens: 100})
	assert.Equal(t, 0.0, cost)

	cost = p.computePhaseCost("1a_crawl", model.TokenUsage{InputTokens: 100})
	assert.Equal(t, 0.0, cost)

	cost = p.computePhaseCost("9_gate", model.TokenUsage{InputTokens: 100})
	assert.Equal(t, 0.0, cost)
}
