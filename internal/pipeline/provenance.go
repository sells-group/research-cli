package pipeline

import (
	"fmt"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/waterfall"
)

// BuildProvenance constructs FieldProvenance records from extraction results.
// It maps each field value back to all extraction attempts and detects overrides
// compared to the previous run's provenance.
func BuildProvenance(
	runID string,
	companyURL string,
	fieldValues map[string]model.FieldValue,
	allAnswers []model.ExtractionAnswer,
	waterfallResult *waterfall.WaterfallResult,
	previousProvenance []model.FieldProvenance,
	_ *model.FieldRegistry,
) []model.FieldProvenance {
	// Index answers by field key for O(1) lookup.
	answersByField := make(map[string][]model.ExtractionAnswer)
	for _, a := range allAnswers {
		answersByField[a.FieldKey] = append(answersByField[a.FieldKey], a)
	}

	// Index previous provenance by field key for override detection.
	prevByField := make(map[string]*model.FieldProvenance)
	for i := range previousProvenance {
		prevByField[previousProvenance[i].FieldKey] = &previousProvenance[i]
	}

	var records []model.FieldProvenance

	for fieldKey, fv := range fieldValues {
		fp := model.FieldProvenance{
			RunID:         runID,
			CompanyURL:    companyURL,
			FieldKey:      fieldKey,
			WinnerSource:  fv.Source,
			WinnerValue:   fmt.Sprintf("%v", fv.Value),
			RawConfidence: fv.Confidence,
			DataAsOf:      fv.DataAsOf,
		}

		// Default effective confidence to raw.
		fp.EffectiveConfidence = fv.Confidence

		// Build attempts from all answers for this field.
		answers := answersByField[fieldKey]
		attempts := make([]model.ProvenanceAttempt, 0, len(answers))
		for _, a := range answers {
			attempts = append(attempts, model.ProvenanceAttempt{
				Source:     a.Source,
				SourceURL:  a.SourceURL,
				Value:      a.Value,
				Confidence: a.Confidence,
				Tier:       a.Tier,
				Reasoning:  a.Reasoning,
				DataAsOf:   a.DataAsOf,
			})
		}
		fp.Attempts = attempts

		// Apply waterfall resolution data if available.
		if waterfallResult != nil {
			if res, ok := waterfallResult.Resolutions[fieldKey]; ok {
				fp.Threshold = res.Threshold
				fp.ThresholdMet = res.ThresholdMet
				fp.PremiumCostUSD = res.PremiumCostUSD
				if res.Winner != nil {
					fp.EffectiveConfidence = res.Winner.EffectiveConfidence
				}
			}
		}

		// Override detection: compare with previous provenance.
		if prev, ok := prevByField[fieldKey]; ok {
			fp.PreviousRunID = prev.RunID
			fp.PreviousValue = prev.WinnerValue
			fp.ValueChanged = fp.WinnerValue != prev.WinnerValue
		}

		records = append(records, fp)
	}

	return records
}

// CountChanged returns the number of provenance records where the value changed.
func CountChanged(records []model.FieldProvenance) int {
	var n int
	for _, r := range records {
		if r.ValueChanged {
			n++
		}
	}
	return n
}
