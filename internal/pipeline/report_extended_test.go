package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestFormatReport_WithPhaseErrors(t *testing.T) {
	company := model.Company{Name: "Error Co", URL: "https://error.com", SalesforceID: "001ERR"}

	phases := []model.PhaseResult{
		{Name: "1a_crawl", Status: model.PhaseStatusComplete, Duration: 1000},
		{Name: "1c_linkedin", Status: model.PhaseStatusFailed, Duration: 500, Error: "connection timeout"},
		{Name: "2_classify", Status: model.PhaseStatusSkipped, Duration: 0},
	}

	report := FormatReport(company, nil, nil, phases, model.TokenUsage{})

	assert.Contains(t, report, "Error Co")
	assert.Contains(t, report, "1a_crawl: complete")
	assert.Contains(t, report, "1c_linkedin: failed")
	assert.Contains(t, report, "Error: connection timeout")
	assert.Contains(t, report, "2_classify: skipped")
}

func TestFormatReport_TierBreakdown_AllTiers(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{Tier: 1},
		{Tier: 1},
		{Tier: 2},
		{Tier: 3},
		{Tier: 3},
		{Tier: 3},
	}

	report := FormatReport(model.Company{Name: "Test"}, answers, nil, nil, model.TokenUsage{})

	assert.Contains(t, report, "Tier 1 (Haiku): 2 answers")
	assert.Contains(t, report, "Tier 2 (Sonnet): 1 answers")
	assert.Contains(t, report, "Tier 3 (Opus): 3 answers")
}

func TestComputeScore_EmptyFieldValues(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a", Required: true},
		{Key: "b", Required: false},
	})

	score := ComputeScore(map[string]model.FieldValue{}, fields, nil)
	assert.Equal(t, 0.0, score)
}
