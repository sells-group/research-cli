package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
)

// MergeAnswers combines answers from all tiers, preferring higher-tier
// answers and higher confidence scores. For each field key, the best
// answer wins.
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

			// Higher tier always wins if it has reasonable confidence.
			if a.Tier > existing.Tier && a.Confidence >= 0.3 {
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
		if field.Validation != "" && !matchesValidation(s, field.Validation) {
			zap.L().Debug("aggregate: validation failed",
				zap.String("field", field.Key),
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
		if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
			return nil
		}
		coerced = s

	case "email":
		s := fmt.Sprintf("%v", value)
		if !strings.Contains(s, "@") {
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
	}
}

// BuildFieldValues validates all answers against the field registry and
// returns a map of field key -> FieldValue.
func BuildFieldValues(answers []model.ExtractionAnswer, fields *model.FieldRegistry) map[string]model.FieldValue {
	result := make(map[string]model.FieldValue)

	for _, a := range answers {
		field := fields.ByKey(a.FieldKey)
		if field == nil {
			continue
		}

		fv := ValidateField(a, field)
		if fv == nil {
			continue
		}
		result[a.FieldKey] = *fv
	}

	return result
}

func matchesValidation(s, pattern string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return true // Invalid pattern: skip validation.
	}
	return re.MatchString(s)
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
