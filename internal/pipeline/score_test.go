package pipeline

import (
	"testing"
	"time"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestScoreConfidence_WeightedAvg(t *testing.T) {
	scoreable := []model.FieldMapping{
		{Key: "industry", Required: true},
		{Key: "revenue", Required: false},
		{Key: "employees", Required: true},
	}
	fv := map[string]model.FieldValue{
		"industry":  {FieldKey: "industry", Confidence: 0.9},
		"employees": {FieldKey: "employees", Confidence: 0.8},
	}
	// (2*0.9 + 0 + 2*0.8) / (2+1+2) = 3.4/5 = 0.68
	assert.InDelta(t, 0.68, scoreConfidence(fv, scoreable), 0.01)
}

func TestScoreConfidence_AllPresent(t *testing.T) {
	scoreable := []model.FieldMapping{
		{Key: "a"}, {Key: "b"},
	}
	fv := map[string]model.FieldValue{
		"a": {Confidence: 1.0},
		"b": {Confidence: 1.0},
	}
	assert.Equal(t, 1.0, scoreConfidence(fv, scoreable))
}

func TestScoreConfidence_NonePresent(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "a"}, {Key: "b"}}
	assert.Equal(t, 0.0, scoreConfidence(nil, scoreable))
}

func TestScoreConfidence_EmptyScoreable(t *testing.T) {
	assert.Equal(t, 0.0, scoreConfidence(nil, nil))
}

func TestScoreConfidence_RequiredWeight(t *testing.T) {
	scoreable := []model.FieldMapping{
		{Key: "req", Required: true},
		{Key: "opt", Required: false},
	}
	fv := map[string]model.FieldValue{
		"req": {Confidence: 0.5},
		"opt": {Confidence: 0.5},
	}
	// (2*0.5 + 1*0.5) / (2+1) = 1.5/3 = 0.5
	assert.InDelta(t, 0.5, scoreConfidence(fv, scoreable), 0.001)
}

func TestScoreCompleteness_AllPresent(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "a"}, {Key: "b"}}
	fv := map[string]model.FieldValue{
		"a": {Confidence: 0.1},
		"b": {Confidence: 0.2},
	}
	assert.Equal(t, 1.0, scoreCompleteness(fv, scoreable))
}

func TestScoreCompleteness_NonePresent(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "a"}, {Key: "b"}}
	assert.Equal(t, 0.0, scoreCompleteness(nil, scoreable))
}

func TestScoreCompleteness_Mixed(t *testing.T) {
	scoreable := []model.FieldMapping{
		{Key: "a", Required: true},
		{Key: "b", Required: false},
	}
	fv := map[string]model.FieldValue{
		"a": {Confidence: 0.3},
	}
	// (2) / (2+1) = 0.666
	assert.InDelta(t, 0.667, scoreCompleteness(fv, scoreable), 0.01)
}

func TestScoreCompleteness_LowConfidenceStillCounts(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "a"}}
	fv := map[string]model.FieldValue{
		"a": {Confidence: 0.01},
	}
	assert.Equal(t, 1.0, scoreCompleteness(fv, scoreable))
}

func TestScoreCompleteness_EmptyScoreable(t *testing.T) {
	assert.Equal(t, 0.0, scoreCompleteness(nil, nil))
}

func TestScoreDiversity_SingleSource(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", SourceURL: "https://acme.com/about", Tier: 1},
	}
	// 1 source → 0.5
	assert.InDelta(t, 0.5, scoreDiversity(fv, answers, scoreable), 0.01)
}

func TestScoreDiversity_TwoSources(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", SourceURL: "https://acme.com/about", Tier: 1},
		{FieldKey: "industry", SourceURL: "https://bbb.org/acme", Tier: 2},
	}
	// 2 sources → 0.75
	assert.InDelta(t, 0.75, scoreDiversity(fv, answers, scoreable), 0.01)
}

func TestScoreDiversity_ThreePlusSources(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", SourceURL: "https://acme.com/about", Tier: 1},
		{FieldKey: "industry", SourceURL: "https://bbb.org/acme", Tier: 2},
		{FieldKey: "industry", SourceURL: "https://sec.gov/edgar", Tier: 3},
	}
	assert.InDelta(t, 1.0, scoreDiversity(fv, answers, scoreable), 0.01)
}

func TestScoreDiversity_ContradictionPenalty(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "revenue", SourceURL: "https://acme.com", Tier: 1, Contradiction: &model.Contradiction{OtherTier: 2}},
		{FieldKey: "revenue", SourceURL: "https://sec.gov", Tier: 2},
	}
	// 2 sources → 0.75, minus 0.2 contradiction → 0.55
	assert.InDelta(t, 0.55, scoreDiversity(fv, answers, scoreable), 0.01)
}

func TestScoreDiversity_ContradictionFloor(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		// Single source (0.5) with contradiction (-0.2) → 0.3, should not go below 0
		{FieldKey: "revenue", SourceURL: "https://acme.com", Tier: 1, Contradiction: &model.Contradiction{OtherTier: 2}},
	}
	assert.InDelta(t, 0.3, scoreDiversity(fv, answers, scoreable), 0.01)
}

func TestScoreDiversity_NoAnswers(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Tier: 1},
	}
	// No answers but field exists → default 0.5
	assert.InDelta(t, 0.5, scoreDiversity(fv, nil, scoreable), 0.01)
}

func TestScoreDiversity_FieldNotPresent(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	// Field not in fieldValues → 0 contribution
	assert.Equal(t, 0.0, scoreDiversity(nil, nil, scoreable))
}

func TestScoreFreshness_NilTimestamp(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "industry"}}
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", DataAsOf: nil},
	}
	assert.Equal(t, 1.0, scoreFreshness(fv, scoreable, time.Now()))
}

func TestScoreFreshness_Within90Days(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * 24 * time.Hour)
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", DataAsOf: &recent},
	}
	assert.Equal(t, 1.0, scoreFreshness(fv, scoreable, now))
}

func TestScoreFreshness_SixMonths(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	sixMonths := now.Add(-180 * 24 * time.Hour)
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", DataAsOf: &sixMonths},
	}
	score := scoreFreshness(fv, scoreable, now)
	// 180 days → linear decay: 1.0 - 0.5*(180-90)/(365-90) = 1.0 - 0.5*90/275 ≈ 0.836
	assert.InDelta(t, 0.836, score, 0.02)
}

func TestScoreFreshness_OneYear(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	oneYear := now.Add(-365 * 24 * time.Hour)
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", DataAsOf: &oneYear},
	}
	score := scoreFreshness(fv, scoreable, now)
	assert.InDelta(t, 0.5, score, 0.01)
}

func TestScoreFreshness_ThreePlusYears(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	old := now.Add(-1200 * 24 * time.Hour)
	scoreable := []model.FieldMapping{{Key: "revenue"}}
	fv := map[string]model.FieldValue{
		"revenue": {FieldKey: "revenue", DataAsOf: &old},
	}
	assert.InDelta(t, 0.2, scoreFreshness(fv, scoreable, now), 0.01)
}

func TestScoreFreshness_Mixed(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	recent := now.Add(-10 * 24 * time.Hour) // within 90d → 1.0
	old := now.Add(-1200 * 24 * time.Hour)  // 3yr+ → 0.2
	scoreable := []model.FieldMapping{
		{Key: "a"},
		{Key: "b"},
	}
	fv := map[string]model.FieldValue{
		"a": {FieldKey: "a", DataAsOf: &recent},
		"b": {FieldKey: "b", DataAsOf: &old},
	}
	// (1*1.0 + 1*0.2) / (1+1) = 0.6
	assert.InDelta(t, 0.6, scoreFreshness(fv, scoreable, now), 0.01)
}

func TestScoreFreshness_FieldNotPresent(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "a"}}
	assert.Equal(t, 0.0, scoreFreshness(nil, scoreable, time.Now()))
}

func TestComputeQualityScore_DefaultWeights(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry"}, {Key: "revenue"},
	})
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Confidence: 0.9, Tier: 1},
		"revenue":  {FieldKey: "revenue", Confidence: 0.8, Tier: 2},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", SourceURL: "https://acme.com", Tier: 1},
		{FieldKey: "revenue", SourceURL: "https://acme.com", Tier: 2},
	}
	weights := config.QualityWeights{Confidence: 0.50, Completeness: 0.25, Diversity: 0.15, Freshness: 0.10}

	bd := computeQualityScore(fv, fields, nil, answers, weights, now)

	assert.InDelta(t, 0.85, bd.Confidence, 0.01) // (0.9+0.8)/2
	assert.Equal(t, 1.0, bd.Completeness)
	assert.InDelta(t, 0.5, bd.Diversity, 0.01) // 1 source each
	assert.Equal(t, 1.0, bd.Freshness)         // nil timestamps
	assert.True(t, bd.Final > 0.6)
}

func TestComputeQualityScore_ConfidenceOnly(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a"}, {Key: "b"},
	})
	fv := map[string]model.FieldValue{
		"a": {FieldKey: "a", Confidence: 0.8},
		"b": {FieldKey: "b", Confidence: 0.6},
	}
	weights := config.QualityWeights{Confidence: 1.0}

	bd := computeQualityScore(fv, fields, nil, nil, weights, time.Now())

	// confidence-only: (0.8+0.6)/2 = 0.7
	assert.InDelta(t, 0.7, bd.Final, 0.01)
	assert.InDelta(t, 0.7, bd.Confidence, 0.01)
}

func TestComputeQualityScore_ZeroWeightsFallback(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a"},
	})
	fv := map[string]model.FieldValue{
		"a": {FieldKey: "a", Confidence: 0.75},
	}
	weights := config.QualityWeights{} // all zero

	bd := computeQualityScore(fv, fields, nil, nil, weights, time.Now())

	// Falls back to confidence-only
	assert.InDelta(t, 0.75, bd.Final, 0.01)
}

func TestComputeQualityScore_EmptyFieldValues(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a"}, {Key: "b"},
	})
	weights := config.QualityWeights{Confidence: 0.5, Completeness: 0.5}

	bd := computeQualityScore(nil, fields, nil, nil, weights, time.Now())

	assert.Equal(t, 0.0, bd.Final)
}

func TestComputeQualityScore_NilRegistry(t *testing.T) {
	weights := config.QualityWeights{Confidence: 1.0}
	bd := computeQualityScore(nil, nil, nil, nil, weights, time.Now())

	assert.Equal(t, 0.0, bd.Final)
}

func TestComputeQualityScore_PerfectScore(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	recent := now.Add(-10 * 24 * time.Hour)
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry"},
	})
	fv := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", Confidence: 1.0, Tier: 1, DataAsOf: &recent},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", SourceURL: "https://acme.com", Tier: 1},
		{FieldKey: "industry", SourceURL: "https://bbb.org", Tier: 2},
		{FieldKey: "industry", SourceURL: "https://sec.gov", Tier: 3},
	}
	weights := config.QualityWeights{Confidence: 0.50, Completeness: 0.25, Diversity: 0.15, Freshness: 0.10}

	bd := computeQualityScore(fv, fields, nil, answers, weights, now)

	// All dimensions should be 1.0
	assert.Equal(t, 1.0, bd.Confidence)
	assert.Equal(t, 1.0, bd.Completeness)
	assert.Equal(t, 1.0, bd.Diversity)
	assert.Equal(t, 1.0, bd.Freshness)
	assert.Equal(t, 1.0, bd.Final)
}

func TestScoreableFields_WithQuestions(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry"},
		{Key: "revenue"},
		{Key: "unrelated"},
	})
	questions := []model.Question{
		{FieldKey: "industry"},
		{FieldKey: "revenue"},
	}

	scoreable := scoreableFields(fields, questions)

	keys := make(map[string]bool)
	for _, f := range scoreable {
		keys[f.Key] = true
	}
	assert.True(t, keys["industry"])
	assert.True(t, keys["revenue"])
	assert.False(t, keys["unrelated"])
}

func TestScoreableFields_NilQuestions(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "a"}, {Key: "b"},
	})

	scoreable := scoreableFields(fields, nil)
	assert.Len(t, scoreable, 2)
}

func TestScoreableFields_AccountNameAutoIncluded(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "account_name"},
		{Key: "industry"},
	})
	questions := []model.Question{
		{FieldKey: "industry"},
	}

	scoreable := scoreableFields(fields, questions)

	keys := make(map[string]bool)
	for _, f := range scoreable {
		keys[f.Key] = true
	}
	assert.True(t, keys["account_name"])
	assert.True(t, keys["industry"])
}

func TestFreshnessDecay_NilReturnsOne(t *testing.T) {
	assert.Equal(t, 1.0, freshnessDecay(nil, time.Now()))
}

func TestFreshnessDecay_ExactBoundaries(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	at90d := now.Add(-90 * 24 * time.Hour)
	assert.Equal(t, 1.0, freshnessDecay(&at90d, now))

	at365d := now.Add(-365 * 24 * time.Hour)
	assert.InDelta(t, 0.5, freshnessDecay(&at365d, now), 0.01)

	at1095d := now.Add(-1095 * 24 * time.Hour)
	assert.InDelta(t, 0.2, freshnessDecay(&at1095d, now), 0.01)

	at2000d := now.Add(-2000 * 24 * time.Hour)
	assert.Equal(t, 0.2, freshnessDecay(&at2000d, now))
}

func TestScoreDiversity_MultiFieldQuestion(t *testing.T) {
	scoreable := []model.FieldMapping{{Key: "city"}, {Key: "state"}}
	fv := map[string]model.FieldValue{
		"city":  {FieldKey: "city", Tier: 1},
		"state": {FieldKey: "state", Tier: 1},
	}
	answers := []model.ExtractionAnswer{
		{FieldKey: "city, state", SourceURL: "https://acme.com", Tier: 1},
		{FieldKey: "city, state", SourceURL: "https://bbb.org", Tier: 2},
	}
	score := scoreDiversity(fv, answers, scoreable)
	// Each field gets 2 sources → 0.75 each
	assert.InDelta(t, 0.75, score, 0.01)
}
