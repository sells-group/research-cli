package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sells-group/research-cli/internal/model"
)

// FormatReport generates a human-readable enrichment report.
func FormatReport(company model.Company, answers []model.ExtractionAnswer, fieldValues map[string]model.FieldValue, phases []model.PhaseResult, totalUsage model.TokenUsage) string {
	var b strings.Builder

	name := company.Name
	if name == "" {
		name = company.URL
	}
	b.WriteString(fmt.Sprintf("# Enrichment Report: %s\n", name))
	b.WriteString(fmt.Sprintf("URL: %s\n", company.URL))
	b.WriteString(fmt.Sprintf("Salesforce ID: %s\n\n", company.SalesforceID))

	// Summary.
	b.WriteString("## Summary\n")
	b.WriteString(fmt.Sprintf("- Fields found: %d\n", len(fieldValues)))
	b.WriteString(fmt.Sprintf("- Total answers: %d\n", len(answers)))
	b.WriteString(fmt.Sprintf("- Token usage: %d input, %d output\n",
		totalUsage.InputTokens, totalUsage.OutputTokens))
	b.WriteString(fmt.Sprintf("- Estimated cost: $%.4f\n\n", totalUsage.Cost))

	// Phase results.
	b.WriteString("## Phases\n")
	for _, p := range phases {
		status := string(p.Status)
		b.WriteString(fmt.Sprintf("- %s: %s (%dms)\n", p.Name, status, p.Duration))
		if p.Error != "" {
			b.WriteString(fmt.Sprintf("  Error: %s\n", p.Error))
		}
	}
	b.WriteString("\n")

	// Field values by tier.
	b.WriteString("## Extracted Fields\n")
	if len(fieldValues) == 0 {
		b.WriteString("No fields extracted.\n\n")
	} else {
		// Sort by field key.
		keys := make([]string, 0, len(fieldValues))
		for k := range fieldValues {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			fv := fieldValues[k]
			b.WriteString(fmt.Sprintf("- **%s** (%s): %v [T%d, %.0f%%]\n",
				fv.FieldKey, fv.SFField, fv.Value, fv.Tier, fv.Confidence*100))
		}
		b.WriteString("\n")
	}

	// Tier breakdown.
	t1Count, t2Count, t3Count := 0, 0, 0
	for _, a := range answers {
		switch a.Tier {
		case 1:
			t1Count++
		case 2:
			t2Count++
		case 3:
			t3Count++
		}
	}
	b.WriteString("## Tier Breakdown\n")
	b.WriteString(fmt.Sprintf("- Tier 1 (Haiku): %d answers\n", t1Count))
	b.WriteString(fmt.Sprintf("- Tier 2 (Sonnet): %d answers\n", t2Count))
	b.WriteString(fmt.Sprintf("- Tier 3 (Opus): %d answers\n", t3Count))

	return b.String()
}

// ComputeScore calculates the quality score based on field coverage and
// confidence. Only fields that have at least one question targeting them
// (or are auto-derived like account_name) count toward the denominator.
// Returns 0.0-1.0.
func ComputeScore(fieldValues map[string]model.FieldValue, fields *model.FieldRegistry, questions []model.Question) float64 {
	if fields == nil || len(fields.Fields) == 0 {
		return 0.0
	}

	// Build set of field keys that have questions targeting them.
	hasQuestion := make(map[string]bool)
	for _, q := range questions {
		for _, fk := range splitFieldKeys(q.FieldKey) {
			hasQuestion[fk] = true
		}
	}
	// Auto-derived fields are always scoreable.
	hasQuestion["account_name"] = true

	totalWeight := 0.0
	score := 0.0

	for _, f := range fields.Fields {
		if len(questions) > 0 && !hasQuestion[f.Key] {
			continue // Skip fields with no question mapping.
		}
		weight := 1.0
		if f.Required {
			weight = 2.0
		}
		totalWeight += weight

		fv, ok := fieldValues[f.Key]
		if ok {
			score += weight * fv.Confidence
		}
	}

	if totalWeight == 0 {
		return 0.0
	}

	return score / totalWeight
}
