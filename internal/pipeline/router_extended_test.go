package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestEscalateQuestions_AllAboveThreshold(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Confidence: 0.8, Value: "Tech"},
		{QuestionID: "q2", FieldKey: "revenue", Confidence: 0.6, Value: "$10M"},
	}

	questions := []model.Question{
		{ID: "q1", Text: "Industry?", FieldKey: "industry", Tier: 1, PageTypes: []model.PageType{model.PageTypeAbout}},
		{ID: "q2", Text: "Revenue?", FieldKey: "revenue", Tier: 1, PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}}},
	}

	escalated := EscalateQuestions(answers, questions, index, 0.4)
	assert.Empty(t, escalated)
}

func TestEscalateQuestions_SomeBelowThreshold(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Confidence: 0.2},           // Low conf + nil value → fails.
		{QuestionID: "q2", FieldKey: "revenue", Confidence: 0.8, Value: "$10M"}, // High conf + value → succeeds.
	}

	questions := []model.Question{
		{ID: "q1", Text: "Industry?", PageTypes: []model.PageType{model.PageTypeAbout}},
		{ID: "q2", Text: "Revenue?", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}}},
	}

	escalated := EscalateQuestions(answers, questions, index, 0.4)
	assert.Len(t, escalated, 1)
	assert.Equal(t, "q1", escalated[0].Question.ID)
}

func TestEscalateQuestions_QuestionNotFoundInMap(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{QuestionID: "nonexistent", FieldKey: "unknown", Confidence: 0.1},
	}

	questions := []model.Question{
		{ID: "q1", Text: "Industry?"},
	}

	index := model.PageIndex{}

	escalated := EscalateQuestions(answers, questions, index, 0.4)
	assert.Empty(t, escalated)
}

func TestEscalateQuestions_NoPagesForQuestion(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Confidence: 0.1},
	}

	questions := []model.Question{
		{ID: "q1", Text: "Industry?", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	// Empty index: no pages match.
	index := model.PageIndex{}

	escalated := EscalateQuestions(answers, questions, index, 0.4)
	assert.Empty(t, escalated)
}

func TestEscalateQuestions_Empty(t *testing.T) {
	escalated := EscalateQuestions(nil, nil, nil, 0.4)
	assert.Empty(t, escalated)
}

func TestRouteQuestions_DefaultTier(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "What?", Tier: 0}, // Default unspecified tier.
	}

	index := model.PageIndex{
		model.PageTypeOther: {{CrawledPage: model.CrawledPage{URL: "https://acme.com"}}},
	}

	batches := RouteQuestions(questions, index)
	// Tier 0 should default to Tier 1.
	assert.Len(t, batches.Tier1, 1)
	assert.Empty(t, batches.Tier2)
	assert.Empty(t, batches.Tier3)
}

func TestRouteQuestions_NoPages(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "What?", Tier: 1, PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	// Empty index.
	index := model.PageIndex{}

	batches := RouteQuestions(questions, index)
	assert.Empty(t, batches.Tier1)
	assert.Len(t, batches.Skipped, 1)
	assert.Equal(t, "no matching pages found", batches.Skipped[0].Reason)
}

func TestRouteQuestions_AllTiers(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "What?", Tier: 1},
		{ID: "q2", Text: "How?", Tier: 2},
		{ID: "q3", Text: "Why?", Tier: 3},
	}

	index := model.PageIndex{
		model.PageTypeOther: {{CrawledPage: model.CrawledPage{URL: "https://acme.com"}}},
	}

	batches := RouteQuestions(questions, index)
	assert.Len(t, batches.Tier1, 1)
	assert.Len(t, batches.Tier2, 1)
	assert.Len(t, batches.Tier3, 1)
}
