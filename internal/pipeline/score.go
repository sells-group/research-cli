package pipeline

import (
	"time"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
)

// ScoreBreakdown holds the individual dimension scores and the final weighted score.
type ScoreBreakdown struct {
	Confidence   float64 `json:"confidence"`
	Completeness float64 `json:"completeness"`
	Diversity    float64 `json:"diversity"`
	Freshness    float64 `json:"freshness"`
	Final        float64 `json:"final"`
}

// computeQualityScore combines four dimension scores into a single 0.0-1.0 score
// using configurable weights. Zero total weight falls back to confidence-only.
func computeQualityScore(fieldValues map[string]model.FieldValue, fields *model.FieldRegistry, questions []model.Question, answers []model.ExtractionAnswer, weights config.QualityWeights, now time.Time) ScoreBreakdown {
	scoreable := scoreableFields(fields, questions)

	conf := scoreConfidence(fieldValues, scoreable)
	comp := scoreCompleteness(fieldValues, scoreable)
	div := scoreDiversity(fieldValues, answers, scoreable)
	fresh := scoreFreshness(fieldValues, scoreable, now)

	totalWeight := weights.Confidence + weights.Completeness + weights.Diversity + weights.Freshness
	if totalWeight == 0 {
		zap.L().Warn("score: all quality weights are zero, falling back to confidence-only")
		// Fallback: confidence-only for backward compat.
		return ScoreBreakdown{
			Confidence:   conf,
			Completeness: comp,
			Diversity:    div,
			Freshness:    fresh,
			Final:        conf,
		}
	}

	final := (weights.Confidence*conf + weights.Completeness*comp + weights.Diversity*div + weights.Freshness*fresh) / totalWeight

	return ScoreBreakdown{
		Confidence:   conf,
		Completeness: comp,
		Diversity:    div,
		Freshness:    fresh,
		Final:        final,
	}
}

// scoreableFields returns the subset of fields that have at least one question
// targeting them (or are auto-derived like account_name).
func scoreableFields(fields *model.FieldRegistry, questions []model.Question) []model.FieldMapping {
	if fields == nil || len(fields.Fields) == 0 {
		return nil
	}

	hasQuestion := make(map[string]bool)
	for _, q := range questions {
		for _, fk := range splitFieldKeys(q.FieldKey) {
			hasQuestion[fk] = true
		}
	}
	hasQuestion["account_name"] = true

	var result []model.FieldMapping
	for _, f := range fields.Fields {
		if len(questions) > 0 && !hasQuestion[f.Key] {
			continue
		}
		result = append(result, f)
	}
	return result
}

// scoreConfidence computes the weighted average of field confidence scores.
// Required fields have weight 2, optional weight 1. Same as the original ComputeScore.
func scoreConfidence(fieldValues map[string]model.FieldValue, scoreable []model.FieldMapping) float64 {
	if len(scoreable) == 0 {
		return 0.0
	}

	totalWeight := 0.0
	score := 0.0

	for _, f := range scoreable {
		weight := 1.0
		if f.Required {
			weight = 2.0
		}
		totalWeight += weight

		if fv, ok := fieldValues[f.Key]; ok {
			score += weight * fv.Confidence
		}
	}

	if totalWeight == 0 {
		return 0.0
	}
	return score / totalWeight
}

// scoreCompleteness computes a binary presence score for fields.
// A field counts as present if it exists in fieldValues (regardless of confidence).
// Required fields have weight 2, optional weight 1.
func scoreCompleteness(fieldValues map[string]model.FieldValue, scoreable []model.FieldMapping) float64 {
	if len(scoreable) == 0 {
		return 0.0
	}

	totalWeight := 0.0
	score := 0.0

	for _, f := range scoreable {
		weight := 1.0
		if f.Required {
			weight = 2.0
		}
		totalWeight += weight

		if _, ok := fieldValues[f.Key]; ok {
			score += weight
		}
	}

	if totalWeight == 0 {
		return 0.0
	}
	return score / totalWeight
}

// scoreDiversity evaluates source diversity across all answers for each field.
// Per-field score: 1 source=0.5, 2 sources=0.75, 3+ sources=1.0.
// Deducts 0.2 if the winning FieldValue's corresponding answer has a Contradiction (floor 0.0).
// Fields with no answers default to 0.5 if present in fieldValues.
func scoreDiversity(fieldValues map[string]model.FieldValue, answers []model.ExtractionAnswer, scoreable []model.FieldMapping) float64 {
	if len(scoreable) == 0 {
		return 0.0
	}

	// Group ALL answers by field key, counting distinct sources.
	type fieldAnswerInfo struct {
		sources map[string]bool
		tiers   map[int]bool
	}
	answersByField := make(map[string]*fieldAnswerInfo)
	for _, a := range answers {
		for _, fk := range splitFieldKeys(a.FieldKey) {
			info, ok := answersByField[fk]
			if !ok {
				info = &fieldAnswerInfo{
					sources: make(map[string]bool),
					tiers:   make(map[int]bool),
				}
				answersByField[fk] = info
			}
			if a.SourceURL != "" {
				info.sources[a.SourceURL] = true
			}
			info.tiers[a.Tier] = true
		}
	}

	// Build a map of winning answers (by field key) to check contradictions.
	winningHasContradiction := make(map[string]bool)
	for _, a := range answers {
		for _, fk := range splitFieldKeys(a.FieldKey) {
			fv, ok := fieldValues[fk]
			if !ok {
				continue
			}
			// Match winning answer by tier (the winning FieldValue carries the tier).
			if a.Tier == fv.Tier && a.Contradiction != nil {
				winningHasContradiction[fk] = true
			}
		}
	}

	totalWeight := 0.0
	score := 0.0

	for _, f := range scoreable {
		weight := 1.0
		if f.Required {
			weight = 2.0
		}
		totalWeight += weight

		if _, ok := fieldValues[f.Key]; !ok {
			continue // field not present — 0 contribution
		}

		var fieldScore float64
		if info, ok := answersByField[f.Key]; ok {
			nSources := len(info.sources)
			switch {
			case nSources >= 3:
				fieldScore = 1.0
			case nSources == 2:
				fieldScore = 0.75
			default:
				fieldScore = 0.5
			}
		} else {
			// Field exists in fieldValues but no answers (e.g. auto-derived).
			fieldScore = 0.5
		}

		if winningHasContradiction[f.Key] {
			fieldScore -= 0.2
			if fieldScore < 0 {
				fieldScore = 0
			}
		}

		score += weight * fieldScore
	}

	if totalWeight == 0 {
		return 0.0
	}
	return score / totalWeight
}

// scoreFreshness evaluates the recency of data using DataAsOf timestamps.
// Full credit (1.0) for data ≤90 days old, linear decay to 0.5 at 1 year,
// floor of 0.2 at 3+ years. Fields with nil DataAsOf get full credit (1.0).
func scoreFreshness(fieldValues map[string]model.FieldValue, scoreable []model.FieldMapping, now time.Time) float64 {
	if len(scoreable) == 0 {
		return 0.0
	}

	totalWeight := 0.0
	score := 0.0

	for _, f := range scoreable {
		weight := 1.0
		if f.Required {
			weight = 2.0
		}
		totalWeight += weight

		fv, ok := fieldValues[f.Key]
		if !ok {
			continue // field not present — 0 contribution
		}

		score += weight * freshnessDecay(fv.DataAsOf, now)
	}

	if totalWeight == 0 {
		return 0.0
	}
	return score / totalWeight
}

// freshnessDecay returns a freshness score for a single field value.
// nil DataAsOf → 1.0, ≤90d → 1.0, linear decay to 0.5 at 365d, floor 0.2 at 1095d+.
func freshnessDecay(dataAsOf *time.Time, now time.Time) float64 {
	if dataAsOf == nil {
		return 1.0
	}

	days := now.Sub(*dataAsOf).Hours() / 24

	switch {
	case days <= 90:
		return 1.0
	case days <= 365:
		// Linear decay from 1.0 at 90d to 0.5 at 365d.
		return 1.0 - 0.5*(days-90)/(365-90)
	case days <= 1095:
		// Linear decay from 0.5 at 365d to 0.2 at 1095d (3 years).
		return 0.5 - 0.3*(days-365)/(1095-365)
	default:
		return 0.2
	}
}
