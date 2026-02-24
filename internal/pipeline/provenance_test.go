package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/waterfall"
)

func TestBuildProvenance_Basic(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			Tier:       1,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			SourceURL:  "https://acme.com/about",
			Tier:       1,
			Reasoning:  "Found on about page",
		},
	}

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "revenue", SFField: "AnnualRevenue", DataType: "currency"},
	})

	records := BuildProvenance("run-1", "https://acme.com", fieldValues, allAnswers, nil, nil, fields)

	require.Len(t, records, 1)
	r := records[0]
	assert.Equal(t, "run-1", r.RunID)
	assert.Equal(t, "https://acme.com", r.CompanyURL)
	assert.Equal(t, "revenue", r.FieldKey)
	assert.Equal(t, "website", r.WinnerSource)
	assert.Equal(t, "5000000", r.WinnerValue)
	assert.InDelta(t, 0.85, r.RawConfidence, 0.001)
	assert.InDelta(t, 0.85, r.EffectiveConfidence, 0.001)
	require.Len(t, r.Attempts, 1)
	assert.Equal(t, "website", r.Attempts[0].Source)
	assert.False(t, r.ValueChanged)
}

func TestBuildProvenance_MultiTierAttempts(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"employees": {
			FieldKey:   "employees",
			Value:      "150",
			Confidence: 0.90,
			Source:     "website",
			Tier:       2,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{
			FieldKey:   "employees",
			Value:      "100",
			Confidence: 0.35,
			Source:     "website",
			Tier:       1,
			Reasoning:  "Rough estimate",
		},
		{
			FieldKey:   "employees",
			Value:      "150",
			Confidence: 0.90,
			Source:     "website",
			Tier:       2,
			Reasoning:  "Multi-page synthesis",
		},
	}

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "employees", SFField: "NumberOfEmployees", DataType: "integer"},
	})

	records := BuildProvenance("run-1", "https://acme.com", fieldValues, allAnswers, nil, nil, fields)

	require.Len(t, records, 1)
	assert.Len(t, records[0].Attempts, 2)
	assert.Equal(t, 1, records[0].Attempts[0].Tier)
	assert.Equal(t, 2, records[0].Attempts[1].Tier)
}

func TestBuildProvenance_OverrideDetection(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "6000000",
			Confidence: 0.90,
			Source:     "website",
			Tier:       1,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "revenue", Value: "6000000", Confidence: 0.90, Source: "website", Tier: 1},
	}
	previousProvenance := []model.FieldProvenance{
		{
			RunID:        "run-old",
			FieldKey:     "revenue",
			WinnerValue:  "5000000",
			WinnerSource: "website",
		},
	}

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "revenue", SFField: "AnnualRevenue", DataType: "currency"},
	})

	records := BuildProvenance("run-2", "https://acme.com", fieldValues, allAnswers, nil, previousProvenance, fields)

	require.Len(t, records, 1)
	assert.True(t, records[0].ValueChanged)
	assert.Equal(t, "5000000", records[0].PreviousValue)
	assert.Equal(t, "run-old", records[0].PreviousRunID)
}

func TestBuildProvenance_NoChange(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			Tier:       1,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "revenue", Value: "5000000", Confidence: 0.85, Source: "website", Tier: 1},
	}
	previousProvenance := []model.FieldProvenance{
		{
			RunID:        "run-old",
			FieldKey:     "revenue",
			WinnerValue:  "5000000",
			WinnerSource: "website",
		},
	}

	fields := model.NewFieldRegistry(nil)
	records := BuildProvenance("run-2", "https://acme.com", fieldValues, allAnswers, nil, previousProvenance, fields)

	require.Len(t, records, 1)
	assert.False(t, records[0].ValueChanged)
	assert.Equal(t, "5000000", records[0].PreviousValue)
	assert.Equal(t, "run-old", records[0].PreviousRunID)
}

func TestBuildProvenance_WithWaterfall(t *testing.T) {
	t.Parallel()

	now := time.Now()
	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			Tier:       1,
			DataAsOf:   &now,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "revenue", Value: "5000000", Confidence: 0.85, Source: "website", Tier: 1},
	}
	wr := &waterfall.WaterfallResult{
		Resolutions: map[string]waterfall.FieldResolution{
			"revenue": {
				FieldKey:     "revenue",
				Resolved:     true,
				Threshold:    0.70,
				ThresholdMet: true,
				Winner: &waterfall.SourceValue{
					Source:              "website",
					Value:               "5000000",
					RawConfidence:       0.85,
					EffectiveConfidence: 0.92,
				},
				PremiumCostUSD: 0.05,
			},
		},
		TotalPremiumUSD: 0.05,
	}

	fields := model.NewFieldRegistry(nil)
	records := BuildProvenance("run-1", "https://acme.com", fieldValues, allAnswers, wr, nil, fields)

	require.Len(t, records, 1)
	assert.InDelta(t, 0.92, records[0].EffectiveConfidence, 0.001)
	assert.InDelta(t, 0.70, records[0].Threshold, 0.001)
	assert.True(t, records[0].ThresholdMet)
	assert.InDelta(t, 0.05, records[0].PremiumCostUSD, 0.001)
}

func TestBuildProvenance_NilWaterfall(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			Tier:       1,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "revenue", Value: "5000000", Confidence: 0.85, Source: "website", Tier: 1},
	}

	fields := model.NewFieldRegistry(nil)
	records := BuildProvenance("run-1", "https://acme.com", fieldValues, allAnswers, nil, nil, fields)

	require.Len(t, records, 1)
	// Effective should equal raw when no waterfall.
	assert.InDelta(t, 0.85, records[0].EffectiveConfidence, 0.001)
	assert.InDelta(t, 0.0, records[0].Threshold, 0.001)
	assert.False(t, records[0].ThresholdMet)
}

func TestBuildProvenance_NoPreviousProvenance(t *testing.T) {
	t.Parallel()

	fieldValues := map[string]model.FieldValue{
		"revenue": {
			FieldKey:   "revenue",
			Value:      "5000000",
			Confidence: 0.85,
			Source:     "website",
			Tier:       1,
		},
	}
	allAnswers := []model.ExtractionAnswer{
		{FieldKey: "revenue", Value: "5000000", Confidence: 0.85, Source: "website", Tier: 1},
	}

	fields := model.NewFieldRegistry(nil)
	records := BuildProvenance("run-1", "https://acme.com", fieldValues, allAnswers, nil, nil, fields)

	require.Len(t, records, 1)
	assert.False(t, records[0].ValueChanged)
	assert.Empty(t, records[0].PreviousValue)
	assert.Empty(t, records[0].PreviousRunID)
}

func TestBuildProvenance_EmptyFieldValues(t *testing.T) {
	t.Parallel()

	fields := model.NewFieldRegistry(nil)
	records := BuildProvenance("run-1", "https://acme.com", nil, nil, nil, nil, fields)
	assert.Empty(t, records)
}

func TestCountChanged(t *testing.T) {
	t.Parallel()

	records := []model.FieldProvenance{
		{FieldKey: "revenue", ValueChanged: true},
		{FieldKey: "employees", ValueChanged: false},
		{FieldKey: "industry", ValueChanged: true},
	}
	assert.Equal(t, 2, CountChanged(records))
}

func TestCountChanged_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, CountChanged(nil))
}
