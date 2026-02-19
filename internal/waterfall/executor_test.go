package waterfall

import (
	"context"
	"testing"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/waterfall/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider implements provider.Provider for testing.
type mockProvider struct {
	name            string
	supportedFields []string
	costPerQuery    float64
	queryResult     *provider.QueryResult
	queryErr        error
	queryCalled     bool
}

func (m *mockProvider) Name() string                   { return m.name }
func (m *mockProvider) SupportedFields() []string      { return m.supportedFields }
func (m *mockProvider) CostPerQuery(_ []string) float64 { return m.costPerQuery }
func (m *mockProvider) CanProvide(fieldKey string) bool {
	for _, f := range m.supportedFields {
		if f == fieldKey {
			return true
		}
	}
	return false
}
func (m *mockProvider) Query(_ context.Context, _ provider.CompanyIdentifier, _ []string) (*provider.QueryResult, error) {
	m.queryCalled = true
	return m.queryResult, m.queryErr
}

func testConfig() *Config {
	return &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay: DecayConfig{
				HalfLifeDays: 365,
				Floor:        0.2,
			},
			MaxPremiumCostUSD: 2.0,
		},
		Fields: map[string]FieldConfig{
			"employee_count": {
				ConfidenceThreshold: 0.65,
				TimeDecay:           &DecayConfig{HalfLifeDays: 180, Floor: 0.15},
				Sources: []SourceConfig{
					{Name: "website_crawl", Tier: 0},
					{Name: "clearbit", Tier: 2},
				},
			},
			"legal_name": {
				ConfidenceThreshold: 0.85,
				TimeDecay:           &DecayConfig{HalfLifeDays: 1825, Floor: 0.3},
				Sources: []SourceConfig{
					{Name: "website_crawl", Tier: 0},
					{Name: "clearbit", Tier: 2},
				},
			},
		},
	}
}

func TestExecutor_HighConfidence_NoDecay(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dataTime := now // Current data — no decay.

	exec := NewExecutor(cfg, nil).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.9,
			Source:     "website_crawl",
			DataAsOf:   &dataTime,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	res := result.Resolutions["employee_count"]
	assert.True(t, res.ThresholdMet)
	assert.True(t, res.Resolved)
	assert.NotNil(t, res.Winner)
	assert.InDelta(t, 0.9, res.Winner.EffectiveConfidence, 0.01)
}

func TestExecutor_DecayedBelowThreshold(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 180 days ago = one half-life for employee_count
	oldData := now.AddDate(0, 0, -180)

	exec := NewExecutor(cfg, nil).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.8,
			Source:     "website_crawl",
			DataAsOf:   &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	res := result.Resolutions["employee_count"]
	// 0.8 * 0.5 = 0.4, below 0.65 threshold
	assert.False(t, res.ThresholdMet)
	assert.InDelta(t, 0.4, res.Winner.EffectiveConfidence, 0.02)
}

func TestExecutor_PremiumProviderCalled(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180) // one half-life

	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"employee_count"},
		costPerQuery:    0.10,
		queryResult: &provider.QueryResult{
			Provider: "clearbit",
			Fields: []provider.FieldResult{
				{FieldKey: "employee_count", Value: 200, Confidence: 0.95, DataAsOf: &now},
			},
			CostUSD: 0.10,
		},
	}

	registry := provider.NewRegistry()
	registry.Register(mock)

	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.8,
			Source:     "website_crawl",
			DataAsOf:   &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	assert.True(t, mock.queryCalled, "premium provider should have been called")
	res := result.Resolutions["employee_count"]
	assert.True(t, res.ThresholdMet)
	assert.Equal(t, "clearbit", res.Winner.Source)
	assert.InDelta(t, 0.95, res.Winner.EffectiveConfidence, 0.01)
	assert.Equal(t, 2, len(res.Attempts)) // original + premium
	assert.Greater(t, result.TotalPremiumUSD, 0.0)
}

func TestExecutor_BudgetExhausted(t *testing.T) {
	cfg := testConfig()
	cfg.Defaults.MaxPremiumCostUSD = 0.05 // Very small budget
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"employee_count"},
		costPerQuery:    0.10, // Exceeds budget
		queryResult: &provider.QueryResult{
			Provider: "clearbit",
			Fields:   []provider.FieldResult{},
			CostUSD:  0.10,
		},
	}

	registry := provider.NewRegistry()
	registry.Register(mock)

	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.8,
			Source:     "website_crawl",
			DataAsOf:   &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	assert.False(t, mock.queryCalled, "premium provider should NOT have been called (budget exceeded)")
	assert.Equal(t, 0.0, result.TotalPremiumUSD)
}

func TestExecutor_NoDataAsOf(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	exec := NewExecutor(cfg, nil).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.8,
			Source:     "website_crawl",
			// No DataAsOf — treated as current.
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	res := result.Resolutions["employee_count"]
	assert.True(t, res.ThresholdMet)
	assert.InDelta(t, 0.8, res.Winner.EffectiveConfidence, 0.01)
}

func TestApplyToFieldValues(t *testing.T) {
	now := time.Now()
	fv := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      100,
			Confidence: 0.4,
			Source:     "old_source",
		},
	}

	wr := &WaterfallResult{
		Resolutions: map[string]FieldResolution{
			"employee_count": {
				FieldKey:     "employee_count",
				ThresholdMet: true,
				Resolved:     true,
				Winner: &SourceValue{
					Source:              "clearbit",
					Value:               200,
					EffectiveConfidence: 0.95,
					DataAsOf:            &now,
					Tier:                2,
				},
			},
			"phone": {
				FieldKey:     "phone",
				ThresholdMet: true,
				Resolved:     true,
				Winner: &SourceValue{
					Source:              "clearbit",
					Value:               "555-1234",
					EffectiveConfidence: 0.85,
					DataAsOf:            &now,
					Tier:                2,
				},
			},
		},
	}

	updated := ApplyToFieldValues(fv, wr)

	// employee_count should be updated with premium source's value, source, and tier.
	assert.InDelta(t, 0.95, updated["employee_count"].Confidence, 0.01)
	assert.Equal(t, "clearbit", updated["employee_count"].Source)
	assert.Equal(t, 200, updated["employee_count"].Value)
	assert.Equal(t, 2, updated["employee_count"].Tier)

	// phone should be added (new field from premium).
	phone, ok := updated["phone"]
	assert.True(t, ok)
	assert.Equal(t, "555-1234", phone.Value)
	assert.Equal(t, "clearbit", phone.Source)
	assert.Equal(t, 2, phone.Tier)
}

func TestApplyToFieldValues_SameSource(t *testing.T) {
	// When winner is from the same source, only confidence should update (no value replacement).
	fv := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      100,
			Confidence: 0.9,
			Source:     "website_crawl",
		},
	}
	wr := &WaterfallResult{
		Resolutions: map[string]FieldResolution{
			"employee_count": {
				FieldKey: "employee_count",
				Winner: &SourceValue{
					Source:              "website_crawl",
					Value:               100,
					EffectiveConfidence: 0.75, // decayed
				},
			},
		},
	}
	updated := ApplyToFieldValues(fv, wr)
	assert.InDelta(t, 0.75, updated["employee_count"].Confidence, 0.01)
	assert.Equal(t, "website_crawl", updated["employee_count"].Source)
	assert.Equal(t, 100, updated["employee_count"].Value)
}

func TestApplyToFieldValues_NilWinner(t *testing.T) {
	fv := map[string]model.FieldValue{
		"phone": {FieldKey: "phone", Value: "555", Confidence: 0.5, Source: "crawl"},
	}
	wr := &WaterfallResult{
		Resolutions: map[string]FieldResolution{
			"phone": {FieldKey: "phone", Winner: nil},
		},
	}
	updated := ApplyToFieldValues(fv, wr)
	assert.Equal(t, "555", updated["phone"].Value)
	assert.Equal(t, 0.5, updated["phone"].Confidence)
}

func TestExecutor_ProviderQueryError(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"employee_count"},
		costPerQuery:    0.10,
		queryErr:        eris.New("connection refused"),
	}

	registry := provider.NewRegistry()
	registry.Register(mock)
	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey: "employee_count", Value: 150, Confidence: 0.8,
			Source: "website_crawl", DataAsOf: &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err, "provider query error should be non-fatal")
	assert.True(t, mock.queryCalled)
	// Original value should be preserved (no premium result merged).
	res := result.Resolutions["employee_count"]
	assert.Equal(t, 1, len(res.Attempts))
	assert.Equal(t, "website_crawl", res.Winner.Source)
}

func TestExecutor_ProviderNotFound(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	// Empty registry — provider "clearbit" is not registered.
	registry := provider.NewRegistry()
	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey: "employee_count", Value: 150, Confidence: 0.8,
			Source: "website_crawl", DataAsOf: &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)
	// Should still evaluate the field, just without premium lookup.
	assert.Equal(t, 0.0, result.TotalPremiumUSD)
}

func TestExecutor_CanProvideFalse(t *testing.T) {
	// Config with only employee_count — no other fields that clearbit might provide.
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
			MaxPremiumCostUSD:   2.0,
		},
		Fields: map[string]FieldConfig{
			"employee_count": {
				ConfidenceThreshold: 0.65,
				TimeDecay:           &DecayConfig{HalfLifeDays: 180, Floor: 0.15},
				Sources: []SourceConfig{
					{Name: "website_crawl", Tier: 0},
					{Name: "clearbit", Tier: 2},
				},
			},
		},
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	// Provider is registered but doesn't support employee_count.
	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"phone"}, // not employee_count
		costPerQuery:    0.10,
		queryResult: &provider.QueryResult{
			Provider: "clearbit",
			Fields:   []provider.FieldResult{},
			CostUSD:  0.10,
		},
	}

	registry := provider.NewRegistry()
	registry.Register(mock)
	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey: "employee_count", Value: 150, Confidence: 0.8,
			Source: "website_crawl", DataAsOf: &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)
	assert.False(t, mock.queryCalled, "should not query provider that can't provide the field")
	assert.Equal(t, 0.0, result.TotalPremiumUSD)
}

func TestExecutor_EmptyFieldsResult(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"employee_count"},
		costPerQuery:    0.10,
		queryResult: &provider.QueryResult{
			Provider: "clearbit",
			Fields:   []provider.FieldResult{}, // empty
			CostUSD:  0.10,
		},
	}

	registry := provider.NewRegistry()
	registry.Register(mock)
	exec := NewExecutor(cfg, registry).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey: "employee_count", Value: 150, Confidence: 0.8,
			Source: "website_crawl", DataAsOf: &oldData,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)
	assert.True(t, mock.queryCalled)
	// Original value preserved since provider returned no fields.
	res := result.Resolutions["employee_count"]
	assert.Equal(t, "website_crawl", res.Winner.Source)
}

func TestExecutor_ContextCancellation(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldData := now.AddDate(0, 0, -180)

	mock := &mockProvider{
		name:            "clearbit",
		supportedFields: []string{"employee_count"},
		costPerQuery:    0.10,
		queryResult: &provider.QueryResult{
			Provider: "clearbit",
			Fields:   []provider.FieldResult{{FieldKey: "employee_count", Value: 200, Confidence: 0.95, DataAsOf: &now}},
			CostUSD:  0.10,
		},
	}

	registry := provider.NewRegistry()
	registry.Register(mock)
	exec := NewExecutor(cfg, registry).WithNow(now)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey: "employee_count", Value: 150, Confidence: 0.8,
			Source: "website_crawl", DataAsOf: &oldData,
		},
	}

	// Should not crash even with cancelled context.
	result, err := exec.Run(ctx, model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestExecutor_FieldTotals(t *testing.T) {
	cfg := testConfig()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	exec := NewExecutor(cfg, nil).WithNow(now)

	fieldValues := map[string]model.FieldValue{
		"employee_count": {
			FieldKey:   "employee_count",
			Value:      150,
			Confidence: 0.9,
			Source:     "website_crawl",
			DataAsOf:   &now,
		},
	}

	result, err := exec.Run(context.Background(), model.Company{URL: "acme.com"}, fieldValues)
	require.NoError(t, err)

	// Should count all fields (from input + configured fields not in input).
	assert.Greater(t, result.FieldsTotal, 0)
	assert.Greater(t, result.FieldsResolved, 0)
}
