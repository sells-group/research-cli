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
	"github.com/sells-group/research-cli/pkg/ppp"
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

			// Higher tier wins if:
			// 1. Existing is null (any value is better than null), OR
			// 2. Both are non-null and new has at least half the existing confidence.
			if a.Tier > existing.Tier && (existing.Value == nil || (a.Value != nil && a.Confidence >= existing.Confidence*0.5)) {
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

	case "number":
		// "number" preserves fractional parts (e.g., ratings like 4.6).
		// Use float when value has a decimal; int otherwise.
		f, ok := toFloat(value)
		if !ok {
			return nil
		}
		if f == float64(int(f)) {
			coerced = int(f)
		} else {
			coerced = f
		}

	case "integer", "int":
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

	case "json":
		// JSON DataType: accept arrays and objects as-is (no coercion needed).
		coerced = value

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
// If company has PreSeeded data, missing fields are gap-filled at lower confidence.
func BuildFieldValues(answers []model.ExtractionAnswer, fields *model.FieldRegistry, company ...model.Company) map[string]model.FieldValue {
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

	// Auto-derive account_name from company_name if not already present.
	// These are conceptually the same field — company_name is extracted,
	// account_name maps to SF Account Name.
	if _, ok := result["account_name"]; !ok {
		if cn, ok := result["company_name"]; ok {
			if nameField := fields.ByKey("account_name"); nameField != nil {
				result["account_name"] = model.FieldValue{
					FieldKey:   "account_name",
					SFField:    nameField.SFField,
					Value:      cn.Value,
					Confidence: cn.Confidence,
					Source:     cn.Source,
					Tier:       cn.Tier,
				}
			}
		}
	}

	// Fill gaps with pre-seeded CSV data (lower confidence than extraction).
	// Also upgrade extracted values when pre-seeded data has more precision
	// (e.g., decimal rating 4.6 vs extracted integer 4).
	if len(company) > 0 && len(company[0].PreSeeded) > 0 {
		for key, val := range company[0].PreSeeded {
			if val == nil || val == "" {
				continue
			}

			existing, alreadyExtracted := result[key]
			if alreadyExtracted {
				// Precision upgrade: if pre-seeded has a decimal value and
				// the extracted value is its integer truncation, use the
				// more precise pre-seeded value. Common for ratings (4.6 vs 4).
				if psFloat, ok := val.(float64); ok && psFloat != float64(int64(psFloat)) {
					if exFloat, ok := toFloat(existing.Value); ok {
						if exFloat == float64(int64(exFloat)) && int64(exFloat) == int64(psFloat) {
							existing.Value = psFloat
							existing.Source += "+precision_upgrade"
							result[key] = existing
						}
					}
				}
				continue
			}

			if field := fields.ByKey(key); field != nil {
				fv := ValidateField(model.ExtractionAnswer{
					FieldKey:   key,
					Value:      val,
					Confidence: 0.6,
					Tier:       0,
				}, field)
				if fv != nil {
					fv.Source = "grata_csv"
					result[key] = *fv
				}
			}
		}
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

// InjectPageMetadata creates field values directly from parsed page metadata
// (e.g. Google Maps review data, phone numbers), bypassing LLM extraction entirely.
// Existing LLM-extracted answers with the same field key are NOT overridden.
// If preSeeded data is provided, Perplexity-sourced values are cross-checked
// against it — large divergences reduce confidence below the gap-fill threshold.
func InjectPageMetadata(answers []model.ExtractionAnswer, pages []model.CrawledPage, preSeeded ...map[string]any) []model.ExtractionAnswer {
	var ps map[string]any
	if len(preSeeded) > 0 {
		ps = preSeeded[0]
	}

	for _, p := range pages {
		if p.Metadata == nil {
			continue
		}

		// Differentiate confidence by metadata source.
		conf := 0.95
		switch p.Metadata.Source {
		case "perplexity":
			conf = 0.70
		case "jina_search":
			conf = 0.85
		case "google_api":
			conf = 0.98
		}

		if p.Metadata.ReviewCount > 0 {
			reviewConf := conf
			// Cross-check Perplexity review count against pre-seeded data.
			if p.Metadata.Source == "perplexity" && ps != nil {
				if psVal, ok := ps["google_reviews_count"]; ok {
					if psCount, ok2 := toNumber(psVal); ok2 && psCount > 0 {
						prox := intProximity(psCount, p.Metadata.ReviewCount)
						if prox < 0.5 {
							reviewConf = 0.50 // Below gap-fill threshold (0.60)
						}
					}
				}
			}
			answers = appendOrUpgrade(answers, "google_reviews_count",
				p.Metadata.ReviewCount, reviewConf, "google_maps_metadata")
		}
		if p.Metadata.Rating > 0 {
			answers = appendOrUpgrade(answers, "google_reviews_rating",
				p.Metadata.Rating, conf, "google_maps_metadata")
		}
		if p.Metadata.Phone != "" {
			answers = appendOrUpgrade(answers, "phone",
				p.Metadata.Phone, 0.90, "website_metadata")
		}
	}
	return answers
}

// intProximity returns 1 - |a-b|/max(a,b), or 1 if both are zero.
func intProximity(a, b int) float64 {
	if a == 0 && b == 0 {
		return 1
	}
	maxVal := a
	if b > maxVal {
		maxVal = b
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return 1 - float64(diff)/float64(maxVal)
}

// appendOrUpgrade adds an answer if no existing answer has the same field key,
// or replaces the existing answer if the new confidence is strictly higher.
func appendOrUpgrade(answers []model.ExtractionAnswer, fieldKey string, value any, confidence float64, source string) []model.ExtractionAnswer {
	for i, a := range answers {
		if a.FieldKey == fieldKey {
			if confidence > a.Confidence {
				answers[i] = model.ExtractionAnswer{
					FieldKey:   fieldKey,
					Value:      value,
					Confidence: confidence,
					Source:     source,
					Tier:       0,
				}
			}
			return answers
		}
	}
	return append(answers, model.ExtractionAnswer{
		FieldKey:   fieldKey,
		Value:      value,
		Confidence: confidence,
		Source:     source,
		Tier:       0,
	})
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
		case "employees", "employee_count", "employee_estimate":
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

// EnrichFromPPP converts PPP loan matches into ExtractionAnswer entries for
// revenue and employees. Uses the best match (index 0, highest approval amount).
// Tier 0 answers won't override higher-tier LLM extractions in MergeAnswers.
func EnrichFromPPP(answers []model.ExtractionAnswer, matches []ppp.LoanMatch) []model.ExtractionAnswer {
	if len(matches) == 0 {
		return answers
	}

	best := matches[0]

	// Revenue estimate: loan amount × 20 multiplier, converted to millions.
	if best.CurrentApproval > 0 {
		revenueMil := best.CurrentApproval * 20 / 1_000_000
		dateApproved := best.DateApproved
		answers = append(answers, model.ExtractionAnswer{
			FieldKey:   "revenue_estimate",
			Value:      fmt.Sprintf("$%.1fM", revenueMil),
			Confidence: best.MatchScore * 0.85,
			Source:     "ppp_database",
			Tier:       0,
			Reasoning:  fmt.Sprintf("PPP loan estimate (loan $%.0f x20)", best.CurrentApproval),
			DataAsOf:   &dateApproved,
		})

		zap.L().Info("aggregate: PPP revenue_estimate added",
			zap.String("borrower", best.BorrowerName),
			zap.Float64("loan_amount", best.CurrentApproval),
			zap.String("revenue_estimate", fmt.Sprintf("$%.1fM", revenueMil)),
			zap.Float64("confidence", best.MatchScore*0.85),
			zap.Int("match_tier", best.MatchTier),
		)
	}

	// Employee count from PPP jobs reported.
	if best.JobsReported > 0 {
		dateApproved := best.DateApproved
		answers = append(answers, model.ExtractionAnswer{
			FieldKey:   "employees",
			Value:      best.JobsReported,
			Confidence: best.MatchScore * 0.7,
			Source:     "ppp_database",
			Tier:       0,
			Reasoning:  "PPP jobs reported",
			DataAsOf:   &dateApproved,
		})

		zap.L().Info("aggregate: PPP employees added",
			zap.String("borrower", best.BorrowerName),
			zap.Int("jobs_reported", best.JobsReported),
			zap.Float64("confidence", best.MatchScore*0.7),
		)
	}

	return answers
}

// CrossValidateEmployeeCount checks if extracted employee count falls within
// LinkedIn's reported range and boosts confidence if so.
func CrossValidateEmployeeCount(answers []model.ExtractionAnswer, linkedInData *LinkedInData) []model.ExtractionAnswer {
	if linkedInData == nil || linkedInData.EmployeeCount == "" {
		return answers
	}
	lo, hi := parseLinkedInRange(linkedInData.EmployeeCount)
	if lo == 0 && hi == 0 {
		return answers
	}
	for i, a := range answers {
		if a.FieldKey == "employees" {
			n, ok := toNumber(a.Value)
			if ok && n >= lo && n <= hi && a.Confidence < 0.85 {
				answers[i].Confidence = 0.85
				answers[i].Source += "+linkedin_validated"
			}
		}
	}
	return answers
}

// parseLinkedInRange parses LinkedIn employee ranges like "51-200", "201-500".
func parseLinkedInRange(s string) (int, int) {
	s = strings.TrimSpace(s)
	// Handle "10,001+" style.
	s = strings.ReplaceAll(s, ",", "")
	if strings.HasSuffix(s, "+") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "+"))
		if err != nil {
			return 0, 0
		}
		return n, 1_000_000 // unbounded upper
	}
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	lo, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	hi, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return lo, hi
}

// MergeContacts consolidates all "contacts" answers from multiple sources into
// a single "contacts" answer. Deduplicates by last name (case-insensitive) and
// caps the array at 3 entries.
func MergeContacts(answers []model.ExtractionAnswer) []model.ExtractionAnswer {
	var merged []map[string]string
	seenLastNames := make(map[string]bool)
	bestConfidence := 0.0
	bestSource := ""
	contactIdx := -1

	for i, a := range answers {
		if a.FieldKey != "contacts" {
			continue
		}
		if contactIdx == -1 {
			contactIdx = i
		}
		if a.Confidence > bestConfidence {
			bestConfidence = a.Confidence
			bestSource = a.Source
		}

		// Handle both []map[string]string and []any (from JSON unmarshaling).
		var items []map[string]string
		switch v := a.Value.(type) {
		case []map[string]string:
			items = v
		case []any:
			for _, item := range v {
				switch m := item.(type) {
				case map[string]string:
					items = append(items, m)
				case map[string]any:
					entry := make(map[string]string)
					for k, val := range m {
						if s, ok := val.(string); ok {
							entry[k] = s
						}
					}
					items = append(items, entry)
				}
			}
		}

		for _, c := range items {
			lastName := strings.ToLower(strings.TrimSpace(c["last_name"]))
			if lastName == "" || seenLastNames[lastName] {
				continue
			}
			seenLastNames[lastName] = true
			merged = append(merged, c)
		}
	}

	if len(merged) == 0 {
		return answers
	}

	// Cap at 3 contacts.
	if len(merged) > 3 {
		merged = merged[:3]
	}

	// Remove all old "contacts" answers and replace with the merged one.
	var result []model.ExtractionAnswer
	replaced := false
	for i, a := range answers {
		if a.FieldKey == "contacts" {
			if !replaced && i == contactIdx {
				result = append(result, model.ExtractionAnswer{
					FieldKey:   "contacts",
					Value:      merged,
					Confidence: bestConfidence,
					Source:     bestSource,
					Tier:       0,
				})
				replaced = true
			}
			continue
		}
		result = append(result, a)
	}
	return result
}

// populateOwnerFromContacts fills owner_* fields from contacts[0] when they
// are missing. This ensures backward compatibility with the single-contact flow.
func populateOwnerFromContacts(result map[string]model.FieldValue, fields *model.FieldRegistry) {
	contactsFV, ok := result["contacts"]
	if !ok {
		return
	}

	// Extract the first contact from the array.
	var first map[string]string
	switch v := contactsFV.Value.(type) {
	case []map[string]string:
		if len(v) == 0 {
			return
		}
		first = v[0]
	case []any:
		if len(v) == 0 {
			return
		}
		switch m := v[0].(type) {
		case map[string]string:
			first = m
		case map[string]any:
			first = make(map[string]string)
			for k, val := range m {
				if s, ok := val.(string); ok {
					first[k] = s
				}
			}
		default:
			return
		}
	default:
		return
	}

	setIfMissing := func(key, contactKey string) {
		if _, exists := result[key]; exists {
			return
		}
		fm := fields.ByKey(key)
		if fm == nil {
			return
		}
		v, ok := first[contactKey]
		if !ok || v == "" {
			return
		}
		result[key] = model.FieldValue{
			FieldKey:   key,
			SFField:    fm.SFField,
			Value:      v,
			Confidence: contactsFV.Confidence,
			Source:     "contacts[0]",
			Tier:       contactsFV.Tier,
		}
	}
	setIfMissing("owner_first_name", "first_name")
	setIfMissing("owner_last_name", "last_name")
	setIfMissing("owner_title", "title")
	setIfMissing("owner_email", "email")
	setIfMissing("owner_phone", "phone")
	setIfMissing("owner_linkedin", "linkedin_url")
}
