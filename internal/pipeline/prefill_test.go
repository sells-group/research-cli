package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
)

func TestResolveADVValue(t *testing.T) {
	t.Parallel()

	aum := int64(500000000)
	employees := 25
	accounts := 150
	secReg := true
	exempt := false
	stateReg := true
	drp := false

	adv := &advRow{
		AUM:             &aum,
		NumEmployees:    &employees,
		NumAccounts:     &accounts,
		SECRegistered:   &secReg,
		ExemptReporting: &exempt,
		StateRegistered: &stateReg,
		HasAnyDRP:       &drp,
	}

	tests := []struct {
		name     string
		fieldKey string
		wantVal  any
		wantOK   bool
	}{
		{"aum_total", "aum_total", int64(500000000), true},
		{"assets_under_management", "assets_under_management", int64(500000000), true},
		{"total_employees", "total_employees", 25, true},
		{"employee_count", "employee_count", 25, true},
		{"num_accounts", "num_accounts", 150, true},
		{"client_count", "client_count", 150, true},
		{"regulatory_status", "regulatory_status", "SEC Registered, State Registered", true},
		{"has_disciplinary_history", "has_disciplinary_history", false, true},
		{"unknown key", "unknown_field", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, ok := resolveADVValue(tt.fieldKey, adv)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantVal, val)
			}
		})
	}
}

func TestResolveADVValue_NilFields(t *testing.T) {
	t.Parallel()

	adv := &advRow{}

	_, ok := resolveADVValue("aum_total", adv)
	assert.False(t, ok)

	_, ok = resolveADVValue("total_employees", adv)
	assert.False(t, ok)

	_, ok = resolveADVValue("num_accounts", adv)
	assert.False(t, ok)

	_, ok = resolveADVValue("has_disciplinary_history", adv)
	assert.False(t, ok)

	// regulatory_status always returns a value (derived)
	val, ok := resolveADVValue("regulatory_status", adv)
	assert.True(t, ok)
	assert.Equal(t, "Unknown", val)
}

func TestDeriveRegulatoryStatus(t *testing.T) {
	t.Parallel()

	t.Run("SEC registered only", func(t *testing.T) {
		t.Parallel()
		sec := true
		adv := &advRow{SECRegistered: &sec}
		assert.Equal(t, "SEC Registered", deriveRegulatoryStatus(adv))
	})

	t.Run("exempt reporting only", func(t *testing.T) {
		t.Parallel()
		exempt := true
		adv := &advRow{ExemptReporting: &exempt}
		assert.Equal(t, "Exempt Reporting Adviser", deriveRegulatoryStatus(adv))
	})

	t.Run("multiple registrations", func(t *testing.T) {
		t.Parallel()
		sec := true
		state := true
		adv := &advRow{SECRegistered: &sec, StateRegistered: &state}
		assert.Equal(t, "SEC Registered, State Registered", deriveRegulatoryStatus(adv))
	})

	t.Run("all nil returns Unknown", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Unknown", deriveRegulatoryStatus(&advRow{}))
	})

	t.Run("all false returns Unknown", func(t *testing.T) {
		t.Parallel()
		f := false
		adv := &advRow{SECRegistered: &f, ExemptReporting: &f, StateRegistered: &f}
		assert.Equal(t, "Unknown", deriveRegulatoryStatus(adv))
	})
}

func TestFilterPrefilledQuestions(t *testing.T) {
	t.Parallel()

	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "aum_total"}},
		{Question: model.Question{ID: "q2", FieldKey: "services_offered"}},
		{Question: model.Question{ID: "q3", FieldKey: "total_employees, num_accounts"}},
		{Question: model.Question{ID: "q4", FieldKey: "year_founded"}},
	}

	prefilled := map[string]bool{
		"aum_total":       true,
		"total_employees": true,
		"num_accounts":    true,
	}

	filtered, skipped := filterPrefilledQuestions(routed, prefilled)

	assert.Equal(t, 2, skipped) // q1 (aum_total) and q3 (both keys prefilled)
	assert.Len(t, filtered, 2)
	assert.Equal(t, "q2", filtered[0].Question.ID)
	assert.Equal(t, "q4", filtered[1].Question.ID)
}

func TestFilterPrefilledQuestions_PartialMultiField(t *testing.T) {
	t.Parallel()

	// If only some keys in a multi-field question are prefilled, keep the question.
	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "aum_total, services_offered"}},
	}

	prefilled := map[string]bool{
		"aum_total": true,
	}

	filtered, skipped := filterPrefilledQuestions(routed, prefilled)

	assert.Equal(t, 0, skipped) // Not all keys prefilled
	assert.Len(t, filtered, 1)
}

func TestFilterPrefilledQuestions_EmptyPrefill(t *testing.T) {
	t.Parallel()

	routed := []model.RoutedQuestion{
		{Question: model.Question{ID: "q1", FieldKey: "aum_total"}},
	}

	filtered, skipped := filterPrefilledQuestions(routed, map[string]bool{})

	assert.Equal(t, 0, skipped)
	assert.Len(t, filtered, 1)
}

func TestPrefilledKeySet(t *testing.T) {
	t.Parallel()

	answers := []model.ExtractionAnswer{
		{FieldKey: "aum_total"},
		{FieldKey: "total_employees"},
		{FieldKey: "aum_total"}, // duplicate
	}

	keys := prefilledKeySet(answers)
	assert.Len(t, keys, 2)
	assert.True(t, keys["aum_total"])
	assert.True(t, keys["total_employees"])
}

func TestSourcingModeQuestionFiltering(t *testing.T) {
	t.Parallel()

	// Simulate the question filtering that happens in sourcing mode.
	questions := []model.Question{
		{ID: "q0", Priority: "P0", FieldKey: "critical"},
		{ID: "q1", Priority: "P1", FieldKey: "important"},
		{ID: "q2", Priority: "P2", FieldKey: "standard"},
		{ID: "q3", Priority: "P3", FieldKey: "low_priority"},
	}

	// Sourcing mode filters to P0+P1.
	filtered := model.FilterByMaxPriority(questions, "P1")
	assert.Len(t, filtered, 2)

	// Verify only P0 and P1 remain.
	for _, q := range filtered {
		assert.True(t, q.Priority == "P0" || q.Priority == "P1",
			"expected P0 or P1, got %s for question %s", q.Priority, q.ID)
	}
}

func TestFormatCRDSource(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "adv_filing (CRD 12345)", FormatCRDSource(12345))
}
