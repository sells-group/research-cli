package pipeline

import (
	"context"
	"fmt"
	"net/mail"
	"net/url"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/estimate"
	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/model"
)

// contradictionThreshold is the minimum confidence on both sides to flag
// a disagreement between tiers.
const contradictionThreshold = 0.5

// MergeAnswers combines answers from all tiers, preferring higher-tier
// answers and higher confidence scores. For each field key, the best
// answer wins. Flags contradictions when tiers disagree with moderate+
// confidence on both sides.
func MergeAnswers(t1, t2, t3 []model.ExtractionAnswer) []model.ExtractionAnswer {
	best := make(map[string]model.ExtractionAnswer)

	// Process in order: T1 first, T2 overrides, T3 overrides.
	for _, answers := range [][]model.ExtractionAnswer{t1, t2, t3} {
		for _, a := range answers {
			if a.FieldKey == "" {
				continue
			}
			existing, ok := best[a.FieldKey]
			if !ok {
				best[a.FieldKey] = a
				continue
			}

			// Check for contradiction: different tiers, both with moderate+
			// confidence, and different values.
			if a.Tier != existing.Tier &&
				a.Confidence >= contradictionThreshold &&
				existing.Confidence >= contradictionThreshold &&
				fmt.Sprintf("%v", a.Value) != fmt.Sprintf("%v", existing.Value) {
				zap.L().Warn("aggregate: tier contradiction detected",
					zap.String("field", a.FieldKey),
					zap.Int("tier_a", existing.Tier),
					zap.Any("value_a", existing.Value),
					zap.Float64("conf_a", existing.Confidence),
					zap.Int("tier_b", a.Tier),
					zap.Any("value_b", a.Value),
					zap.Float64("conf_b", a.Confidence),
				)
				// Attach contradiction metadata to the winner.
				a.Contradiction = &model.Contradiction{
					OtherTier:       existing.Tier,
					OtherValue:      existing.Value,
					OtherConfidence: existing.Confidence,
				}
			}

			// Higher tier wins if existing is null or new has at least half the existing confidence.
			if a.Tier > existing.Tier && (existing.Value == nil || a.Confidence >= existing.Confidence*0.5) {
				best[a.FieldKey] = a
				continue
			}

			// Same tier: higher confidence wins.
			if a.Tier == existing.Tier && a.Confidence > existing.Confidence {
				best[a.FieldKey] = a
			}
		}
	}

	result := make([]model.ExtractionAnswer, 0, len(best))
	for _, a := range best {
		result = append(result, a)
	}
	return result
}

// ValidateField checks an answer against its field mapping and returns
// a cleaned FieldValue. Returns nil if validation fails.
func ValidateField(answer model.ExtractionAnswer, field *model.FieldMapping) *model.FieldValue {
	if field == nil {
		return nil
	}

	value := answer.Value
	if value == nil {
		return nil
	}

	// Type coercion and validation.
	var coerced any
	switch strings.ToLower(field.DataType) {
	case "string", "text":
		s := fmt.Sprintf("%v", value)
		if field.MaxLength > 0 && len(s) > field.MaxLength {
			s = s[:field.MaxLength]
		}
		if field.ValidationRegex != nil && !field.ValidationRegex.MatchString(s) {
			zap.L().Warn("aggregate: validation failed",
				zap.String("field", field.Key),
				zap.String("raw_value", s),
				zap.String("validation", field.Validation),
			)
			return nil
		}
		coerced = s

	case "number", "integer", "int":
		n, ok := toNumber(value)
		if !ok {
			return nil
		}
		coerced = n

	case "float", "double", "decimal", "currency":
		f, ok := toFloat(value)
		if !ok {
			return nil
		}
		coerced = f

	case "boolean", "bool":
		b, ok := toBool(value)
		if !ok {
			return nil
		}
		coerced = b

	case "url":
		s := fmt.Sprintf("%v", value)
		u, parseErr := url.Parse(s)
		if parseErr != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			zap.L().Warn("aggregate: invalid URL",
				zap.String("field", field.Key),
				zap.String("raw_value", s),
			)
			return nil
		}
		coerced = s

	case "email":
		s := fmt.Sprintf("%v", value)
		if _, parseErr := mail.ParseAddress(s); parseErr != nil {
			zap.L().Warn("aggregate: invalid email",
				zap.String("field", field.Key),
				zap.String("raw_value", s),
			)
			return nil
		}
		coerced = s

	case "phone":
		s := fmt.Sprintf("%v", value)
		// Strip non-numeric except + and spaces.
		cleaned := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' || r == '+' || r == ' ' || r == '-' || r == '(' || r == ')' {
				return r
			}
			return -1
		}, s)
		if len(cleaned) < 7 {
			return nil
		}
		coerced = cleaned

	default:
		coerced = value
	}

	return &model.FieldValue{
		FieldKey:   field.Key,
		SFField:    field.SFField,
		Value:      coerced,
		Confidence: answer.Confidence,
		Source:     answer.SourceURL,
		Tier:       answer.Tier,
		DataAsOf:   answer.DataAsOf,
	}
}

// BuildFieldValues validates all answers against the field registry and
// returns a map of field key -> FieldValue. Logs a summary of validation failures.
func BuildFieldValues(answers []model.ExtractionAnswer, fields *model.FieldRegistry) map[string]model.FieldValue {
	result := make(map[string]model.FieldValue)
	var failures int

	for _, a := range answers {
		field := fields.ByKey(a.FieldKey)
		if field == nil {
			continue
		}

		fv := ValidateField(a, field)
		if fv == nil {
			failures++
			continue
		}
		result[a.FieldKey] = *fv
	}

	if failures > 0 {
		zap.L().Warn("aggregate: field validation summary",
			zap.Int("validation_failures", failures),
			zap.Int("fields_valid", len(result)),
			zap.Int("answers_total", len(answers)),
		)
	}

	return result
}

func toNumber(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case float32:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case string:
		// Strip commas and try to parse.
		cleaned := strings.ReplaceAll(n, ",", "")
		i, err := strconv.Atoi(cleaned)
		if err != nil {
			// Try parsing as float first.
			f, err := strconv.ParseFloat(cleaned, 64)
			if err != nil {
				return 0, false
			}
			return int(f), true
		}
		return i, true
	default:
		return 0, false
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		cleaned := strings.ReplaceAll(n, ",", "")
		cleaned = strings.TrimPrefix(cleaned, "$")
		f, err := strconv.ParseFloat(cleaned, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func toBool(v any) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	case string:
		lower := strings.ToLower(b)
		switch lower {
		case "true", "yes", "1":
			return true, true
		case "false", "no", "0":
			return false, true
		}
		return false, false
	case float64:
		return b != 0, true
	case int:
		return b != 0, true
	default:
		return false, false
	}
}

// EnrichWithRevenueEstimate adds a revenue estimate to the merged answers
// when employee_count and naics_code are present and the estimator is available.
// It only adds the estimate if no existing revenue_range answer has high confidence.
func EnrichWithRevenueEstimate(ctx context.Context, answers []model.ExtractionAnswer, company model.Company, estimator *estimate.RevenueEstimator) []model.ExtractionAnswer {
	if estimator == nil {
		return answers
	}

	// Find employee count and NAICS code from existing answers.
	var empCount int
	var naicsCode string
	var hasHighConfRevenue bool

	for _, a := range answers {
		switch a.FieldKey {
		case "employee_count", "employee_estimate":
			if n, ok := toNumber(a.Value); ok && n > 0 {
				empCount = n
			}
		case "naics_code":
			if s, ok := a.Value.(string); ok && s != "" {
				naicsCode = s
			}
		case "revenue_range":
			if a.Confidence >= 0.7 {
				hasHighConfRevenue = true
			}
		}
	}

	if empCount == 0 || naicsCode == "" {
		return answers
	}
	if hasHighConfRevenue {
		zap.L().Debug("aggregate: skipping revenue estimate, high-confidence revenue_range exists")
		return answers
	}

	// Convert state abbreviation to FIPS code.
	stateFIPS := transform.StateAbbrToFIPS[strings.ToUpper(company.State)]

	est, err := estimator.Estimate(ctx, naicsCode, stateFIPS, empCount)
	if err != nil {
		zap.L().Warn("aggregate: revenue estimation failed",
			zap.String("company", company.Name),
			zap.Error(err),
		)
		return answers
	}

	answers = append(answers, model.ExtractionAnswer{
		FieldKey:   "revenue_estimate",
		Value:      est.Amount,
		Confidence: est.Confidence,
		Source:     fmt.Sprintf("CBP %d NAICS %s", est.Year, est.NAICSUsed),
		Tier:       0,
	})
	answers = append(answers, model.ExtractionAnswer{
		FieldKey:   "revenue_confidence",
		Value:      est.Confidence,
		Confidence: est.Confidence,
		Source:     fmt.Sprintf("CBP %d NAICS %s", est.Year, est.NAICSUsed),
		Tier:       0,
	})

	zap.L().Info("aggregate: revenue estimate added",
		zap.String("company", company.Name),
		zap.Int64("revenue", est.Amount),
		zap.Float64("confidence", est.Confidence),
		zap.String("naics_used", est.NAICSUsed),
	)

	return answers
}
