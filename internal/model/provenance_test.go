package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldProvenance_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	fp := FieldProvenance{
		ID:                  1,
		RunID:               "run-abc",
		CompanyURL:          "https://acme.com",
		FieldKey:            "revenue",
		WinnerSource:        "website",
		WinnerValue:         "5000000",
		RawConfidence:       0.85,
		EffectiveConfidence: 0.90,
		DataAsOf:            &now,
		Threshold:           0.70,
		ThresholdMet:        true,
		Attempts: []ProvenanceAttempt{
			{
				Source:     "website",
				SourceURL:  "https://acme.com/about",
				Value:      "5000000",
				Confidence: 0.85,
				Tier:       1,
				Reasoning:  "Found on about page",
				DataAsOf:   &now,
			},
			{
				Source:     "linkedin",
				Value:      "4500000",
				Confidence: 0.60,
				Tier:       2,
				Reasoning:  "LinkedIn estimate",
			},
		},
		PremiumCostUSD: 0.05,
		PreviousValue:  "4000000",
		PreviousRunID:  "run-xyz",
		ValueChanged:   true,
		CreatedAt:      now,
	}

	data, err := json.Marshal(fp)
	require.NoError(t, err)

	var decoded FieldProvenance
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, fp.RunID, decoded.RunID)
	assert.Equal(t, fp.CompanyURL, decoded.CompanyURL)
	assert.Equal(t, fp.FieldKey, decoded.FieldKey)
	assert.Equal(t, fp.WinnerSource, decoded.WinnerSource)
	assert.Equal(t, fp.WinnerValue, decoded.WinnerValue)
	assert.InDelta(t, fp.RawConfidence, decoded.RawConfidence, 0.001)
	assert.InDelta(t, fp.EffectiveConfidence, decoded.EffectiveConfidence, 0.001)
	assert.True(t, decoded.ThresholdMet)
	assert.True(t, decoded.ValueChanged)
	assert.Equal(t, fp.PreviousValue, decoded.PreviousValue)
	assert.Equal(t, fp.PreviousRunID, decoded.PreviousRunID)
	require.Len(t, decoded.Attempts, 2)
	assert.Equal(t, "website", decoded.Attempts[0].Source)
	assert.Equal(t, 1, decoded.Attempts[0].Tier)
	assert.Equal(t, "linkedin", decoded.Attempts[1].Source)
	assert.Equal(t, 2, decoded.Attempts[1].Tier)
}

func TestProvenanceAttempt_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	a := ProvenanceAttempt{
		Source:     "website",
		SourceURL:  "https://acme.com/about",
		Value:      "test_value",
		Confidence: 0.75,
		Tier:       1,
		Reasoning:  "Extracted from about page",
		DataAsOf:   &now,
	}

	data, err := json.Marshal(a)
	require.NoError(t, err)

	var decoded ProvenanceAttempt
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, a.Source, decoded.Source)
	assert.Equal(t, a.SourceURL, decoded.SourceURL)
	assert.InDelta(t, a.Confidence, decoded.Confidence, 0.001)
	assert.Equal(t, a.Tier, decoded.Tier)
	assert.Equal(t, a.Reasoning, decoded.Reasoning)
	assert.NotNil(t, decoded.DataAsOf)
}

func TestFieldProvenance_NilAttempts(t *testing.T) {
	t.Parallel()

	fp := FieldProvenance{
		RunID:      "run-1",
		CompanyURL: "https://test.com",
		FieldKey:   "employees",
	}

	data, err := json.Marshal(fp)
	require.NoError(t, err)

	var decoded FieldProvenance
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Nil(t, decoded.Attempts)
}
