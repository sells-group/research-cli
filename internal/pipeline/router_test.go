package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestRouteQuestions_BasicRouting(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "What does the company do?", Tier: 1, FieldKey: "description", PageTypes: []model.PageType{model.PageTypeAbout}},
		{ID: "q2", Text: "What are their services?", Tier: 1, FieldKey: "services", PageTypes: []model.PageType{model.PageTypeServices}},
		{ID: "q3", Text: "Synthesize revenue data", Tier: 2, FieldKey: "revenue", PageTypes: []model.PageType{model.PageTypeAbout, model.PageTypeInvestors}},
		{ID: "q4", Text: "Expert analysis", Tier: 3, FieldKey: "strategy", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Title: "About"}},
		},
		model.PageTypeServices: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/services", Title: "Services"}},
		},
	}

	batches := RouteQuestions(questions, index)

	assert.Len(t, batches.Tier1, 2)
	assert.Len(t, batches.Tier2, 1)
	assert.Len(t, batches.Tier3, 1)
	assert.Len(t, batches.Skipped, 0)
}

func TestRouteQuestions_SkippedWhenNoPages(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "What about pricing?", Tier: 1, FieldKey: "pricing", PageTypes: []model.PageType{model.PageTypePricing}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Title: "About"}},
		},
	}

	batches := RouteQuestions(questions, index)

	assert.Len(t, batches.Tier1, 0)
	assert.Len(t, batches.Skipped, 1)
	assert.Equal(t, "no matching pages found", batches.Skipped[0].Reason)
}

func TestRouteQuestions_NoPageTypes_MatchesAll(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "General question", Tier: 1, FieldKey: "general"},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Title: "About"}},
		},
		model.PageTypeServices: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/services", Title: "Services"}},
		},
	}

	batches := RouteQuestions(questions, index)

	assert.Len(t, batches.Tier1, 1)
	assert.Len(t, batches.Tier1[0].Pages, 2)
}

func TestRouteQuestions_DefaultTierIs1(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "No tier set", Tier: 0, FieldKey: "field1"},
	}

	index := model.PageIndex{
		model.PageTypeHomepage: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com", Title: "Home"}},
		},
	}

	batches := RouteQuestions(questions, index)
	assert.Len(t, batches.Tier1, 1)
}

func TestRouteQuestions_EmptyQuestions(t *testing.T) {
	index := model.PageIndex{
		model.PageTypeAbout: {{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}}},
	}

	batches := RouteQuestions(nil, index)
	assert.Len(t, batches.Tier1, 0)
	assert.Len(t, batches.Tier2, 0)
	assert.Len(t, batches.Tier3, 0)
	assert.Len(t, batches.Skipped, 0)
}

func TestEscalateQuestions(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Tier: 1, FieldKey: "field1", PageTypes: []model.PageType{model.PageTypeAbout}},
		{ID: "q2", Tier: 1, FieldKey: "field2", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}},
		},
	}

	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "field1", Confidence: 0.2, Tier: 1},  // Low confidence.
		{QuestionID: "q2", FieldKey: "field2", Confidence: 0.8, Tier: 1},  // High confidence.
	}

	escalated := EscalateQuestions(answers, questions, index, 0.4)

	assert.Len(t, escalated, 1)
	assert.Equal(t, "q1", escalated[0].Question.ID)
}

func TestEscalateQuestions_NoneEscalated(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Tier: 1, FieldKey: "f1", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}},
		},
	}

	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "f1", Confidence: 0.9, Tier: 1},
	}

	escalated := EscalateQuestions(answers, questions, index, 0.4)
	assert.Len(t, escalated, 0)
}

func TestRouteQuestions_ExternalPagesIncludedWithPageTypes(t *testing.T) {
	questions := []model.Question{
		{ID: "q1", Text: "When was the company founded?", Tier: 1, FieldKey: "year_founded", PageTypes: []model.PageType{model.PageTypeAbout}},
	}

	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about", Title: "About"}},
		},
		model.PageTypeBBB: {
			{CrawledPage: model.CrawledPage{URL: "https://bbb.org/acme", Title: "[bbb] Acme Corp"}},
		},
		model.PageTypeGoogleMaps: {
			{CrawledPage: model.CrawledPage{URL: "https://maps.google.com/acme", Title: "[google_maps] Acme"}},
		},
	}

	batches := RouteQuestions(questions, index)

	assert.Len(t, batches.Tier1, 1)
	// Should include about page + BBB + Google Maps = 3 pages.
	assert.Len(t, batches.Tier1[0].Pages, 3)
	assert.Len(t, batches.Skipped, 0)
}

func TestRouteQuestions_ExternalPagesNotDuplicated(t *testing.T) {
	// If question already includes an external page type, don't duplicate.
	questions := []model.Question{
		{ID: "q1", Text: "Reputation?", Tier: 2, FieldKey: "reputation", PageTypes: []model.PageType{model.PageTypeGoogleMaps}},
	}

	index := model.PageIndex{
		model.PageTypeGoogleMaps: {
			{CrawledPage: model.CrawledPage{URL: "https://maps.google.com/acme", Title: "[google_maps] Acme"}},
		},
		model.PageTypeBBB: {
			{CrawledPage: model.CrawledPage{URL: "https://bbb.org/acme", Title: "[bbb] Acme"}},
		},
	}

	batches := RouteQuestions(questions, index)

	assert.Len(t, batches.Tier2, 1)
	// Google Maps (from PageTypes) + BBB (from external supplement) = 2 pages.
	assert.Len(t, batches.Tier2[0].Pages, 2)
}

func TestFindPagesForQuestion_Deduplication(t *testing.T) {
	q := model.Question{
		PageTypes: []model.PageType{model.PageTypeAbout, model.PageTypeHomepage},
	}

	// Same URL appears in both page types.
	index := model.PageIndex{
		model.PageTypeAbout: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}},
		},
		model.PageTypeHomepage: {
			{CrawledPage: model.CrawledPage{URL: "https://acme.com/about"}},
		},
	}

	pages := findPagesForQuestion(q, index)
	assert.Len(t, pages, 1)
}
