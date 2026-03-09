package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentifierToDataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		system      string
		wantDataset string
		wantCol     string
	}{
		{"crd", "adv_firms", "crd_number"},
		{"cik", "edgar_entities", "cik"},
		{"ein", "form_5500", "ack_id"},
		{"fdic", "fdic_institutions", "cert"},
		{"ncua", "ncua_call_reports", "cu_number"},
		{"unknown", "", ""},
		{"", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.system, func(t *testing.T) {
			t.Parallel()
			dataset, col := identifierToDataset(tt.system)
			assert.Equal(t, tt.wantDataset, dataset)
			assert.Equal(t, tt.wantCol, col)
		})
	}
}

func TestBuildIdentifiersMap(t *testing.T) {
	t.Parallel()

	t.Run("extracts known identifiers", func(t *testing.T) {
		t.Parallel()
		preSeeded := map[string]any{
			"crd_number":     12345,
			"cik":            "0001234567",
			"ein":            "12-3456789",
			"fdic_cert":      9876,
			"ncua_cu_number": 5432,
			"naics_code":     "523930", // should be ignored
		}
		ids := buildIdentifiersMap(preSeeded)
		assert.Equal(t, "12345", ids["crd"])
		assert.Equal(t, "0001234567", ids["cik"])
		assert.Equal(t, "12-3456789", ids["ein"])
		assert.Equal(t, "9876", ids["fdic"])
		assert.Equal(t, "5432", ids["ncua"])
		assert.NotContains(t, ids, "naics_code")
	})

	t.Run("empty preseeded returns empty map", func(t *testing.T) {
		t.Parallel()
		ids := buildIdentifiersMap(nil)
		assert.Empty(t, ids)
	})

	t.Run("no relevant identifiers returns empty map", func(t *testing.T) {
		t.Parallel()
		ids := buildIdentifiersMap(map[string]any{
			"employee_count": 50,
			"naics_code":     "523930",
		})
		assert.Empty(t, ids)
	})
}

func TestFormatLargeNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1000000, "1,000,000"},
		{1234567890, "1,234,567,890"},
		{-5000, "-5,000"},
		{100, "100"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			got := formatLargeNumber(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFederalContextToPage(t *testing.T) {
	t.Parallel()

	fc := &FederalContext{
		Summary: "[SEC ADV Filing Data]\nRegistered Name: Test Advisors LLC",
		EntityMatches: []FedEntityMatch{
			{Dataset: "adv_firms", EntityID: "12345", Confidence: 1.0},
		},
	}

	page := federalContextToPage(fc)
	assert.Equal(t, "federal_data://entity_xref", page.URL)
	assert.Equal(t, "Federal Data Cross-Reference", page.Title)
	assert.Contains(t, page.Markdown, "# Federal Data Cross-Reference")
	assert.Contains(t, page.Markdown, "Test Advisors LLC")
	assert.Equal(t, 200, page.StatusCode)
}

func TestLookupFederalContext_NilPool(t *testing.T) {
	t.Parallel()

	fc, err := LookupFederalContext(t.Context(), nil, map[string]string{"crd": "12345"})
	require.NoError(t, err)
	assert.Nil(t, fc)
}

func TestLookupFederalContext_EmptyIdentifiers(t *testing.T) {
	t.Parallel()

	// Even with a non-nil pool, empty identifiers should return nil.
	// We can't pass a real pool here without pgxmock, but the nil check
	// on identifiers happens first.
	fc, err := LookupFederalContext(t.Context(), nil, map[string]string{})
	require.NoError(t, err)
	assert.Nil(t, fc)
}
