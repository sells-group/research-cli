package pipeline

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
)

// FormatReport generates a human-readable enrichment report.
func FormatReport(company model.Company, answers []model.ExtractionAnswer, fieldValues map[string]model.FieldValue, phases []model.PhaseResult, totalUsage model.TokenUsage) string {
	var b strings.Builder

	name := company.Name
	if name == "" {
		name = company.URL
	}
	fmt.Fprintf(&b, "# Enrichment Report: %s\n", name)
	fmt.Fprintf(&b, "URL: %s\n", company.URL)
	fmt.Fprintf(&b, "Salesforce ID: %s\n\n", company.SalesforceID)

	// Summary.
	b.WriteString("## Summary\n")
	fmt.Fprintf(&b, "- Fields found: %d\n", len(fieldValues))
	fmt.Fprintf(&b, "- Total answers: %d\n", len(answers))
	fmt.Fprintf(&b, "- Token usage: %d input, %d output\n",
		totalUsage.InputTokens, totalUsage.OutputTokens)
	fmt.Fprintf(&b, "- Estimated cost: $%.4f\n\n", totalUsage.Cost)

	// Phase results.
	b.WriteString("## Phases\n")
	for _, p := range phases {
		status := string(p.Status)
		fmt.Fprintf(&b, "- %s: %s (%dms)\n", p.Name, status, p.Duration)
		if p.Error != "" {
			fmt.Fprintf(&b, "  Error: %s\n", p.Error)
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
			fmt.Fprintf(&b, "- **%s** (%s): %v [T%d, %.0f%%]\n",
				fv.FieldKey, fv.SFField, fv.Value, fv.Tier, fv.Confidence*100)
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
	fmt.Fprintf(&b, "- Tier 1 (Haiku): %d answers\n", t1Count)
	fmt.Fprintf(&b, "- Tier 2 (Sonnet): %d answers\n", t2Count)
	fmt.Fprintf(&b, "- Tier 3 (Opus): %d answers\n", t3Count)

	return b.String()
}

// ComputeScore calculates a multi-dimension quality score based on confidence,
// completeness, source diversity, and data freshness. Only fields that have at
// least one question targeting them (or are auto-derived like account_name)
// count toward the score. Returns a ScoreBreakdown with Final in 0.0-1.0.
func ComputeScore(fieldValues map[string]model.FieldValue, fields *model.FieldRegistry, questions []model.Question, answers []model.ExtractionAnswer, weights config.QualityWeights) ScoreBreakdown {
	if fields == nil || len(fields.Fields) == 0 {
		return ScoreBreakdown{}
	}
	return computeQualityScore(fieldValues, fields, questions, answers, weights, time.Now())
}
