package pipeline

import (
	"strings"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestFormatReport(t *testing.T) {
	company := model.Company{
		Name:         "Acme Corp",
		URL:          "https://acme.com",
		SalesforceID: "001ABC",
	}

	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		{QuestionID: "q2", FieldKey: "revenue", Value: "$10M", Confidence: 0.7, Tier: 2},
	}

	fieldValues := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		"revenue":  {FieldKey: "revenue", SFField: "AnnualRevenue", Value: "$10M", Confidence: 0.7, Tier: 2},
	}

	phases := []model.PhaseResult{
		{Name: "1a_crawl", Status: model.PhaseStatusComplete, Duration: 1000},
		{Name: "2_classify", Status: model.PhaseStatusComplete, Duration: 500},
	}

	usage := model.TokenUsage{InputTokens: 10000, OutputTokens: 2000}

	report := FormatReport(company, answers, fieldValues, phases, usage)

	assert.Contains(t, report, "Acme Corp")
	assert.Contains(t, report, "https://acme.com")
	assert.Contains(t, report, "001ABC")
	assert.Contains(t, report, "Fields found: 2")
	assert.Contains(t, report, "Total answers: 2")
	assert.Contains(t, report, "1a_crawl")
	assert.Contains(t, report, "industry")
	assert.Contains(t, report, "revenue")
	assert.Contains(t, report, "Tier 1 (Haiku): 1 answers")
	assert.Contains(t, report, "Tier 2 (Sonnet): 1 answers")
}

func TestFormatReport_EmptyFieldValues(t *testing.T) {
	company := model.Company{Name: "Empty Co"}
	report := FormatReport(company, nil, nil, nil, model.TokenUsage{})

	assert.Contains(t, report, "Empty Co")
	assert.Contains(t, report, "No fields extracted")
}

func TestComputeScore(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", Required: true},
		{Key: "revenue", Required: false},
		{Key: "employees", Required: true},
	})

	fieldValues := map[string]model.FieldValue{
		"industry":  {FieldKey: "industry", Confidence: 0.9},
		"employees": {FieldKey: "employees", Confidence: 0.8},
	}

	score := ComputeScore(fieldValues, fields, nil)

	// Required fields have weight 2, non-required weight 1.
	// Total weight: 2 + 1 + 2 = 5
	// Score: (2*0.9 + 0 + 2*0.8) / 5 = (1.8 + 1.6) / 5 = 3.4/5 = 0.68
	assert.InDelta(t, 0.68, score, 0.01)
}

func TestComputeScore_AllFieldsPresent(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a", Required: false},
		{Key: "b", Required: false},
	})

	fieldValues := map[string]model.FieldValue{
		"a": {FieldKey: "a", Confidence: 1.0},
		"b": {FieldKey: "b", Confidence: 1.0},
	}

	score := ComputeScore(fieldValues, fields, nil)
	assert.Equal(t, 1.0, score)
}

func TestComputeScore_NoFields(t *testing.T) {
	fields := model.NewFieldRegistry(nil)
	score := ComputeScore(nil, fields, nil)
	assert.Equal(t, 0.0, score)
}

func TestComputeScore_NilRegistry(t *testing.T) {
	score := ComputeScore(nil, nil, nil)
	assert.Equal(t, 0.0, score)
}

func TestFormatReport_HasHeaders(t *testing.T) {
	company := model.Company{Name: "Test Co", URL: "https://test.com"}
	report := FormatReport(company, nil, map[string]model.FieldValue{}, nil, model.TokenUsage{})

	assert.True(t, strings.HasPrefix(report, "# Enrichment Report: Test Co"))
	assert.Contains(t, report, "## Summary")
	assert.Contains(t, report, "## Phases")
	assert.Contains(t, report, "## Extracted Fields")
	assert.Contains(t, report, "## Tier Breakdown")
}
