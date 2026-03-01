package geoscraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCategory_String(t *testing.T) {
	tests := []struct {
		cat  Category
		want string
	}{
		{National, "national"},
		{State, "state"},
		{OnDemand, "on_demand"},
		{Category(99), "unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.cat.String())
	}
}

func TestParseCategory(t *testing.T) {
	tests := []struct {
		input string
		want  Category
	}{
		{"national", National},
		{"state", State},
		{"on_demand", OnDemand},
		{"on-demand", OnDemand},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseCategory(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCategory_Invalid(t *testing.T) {
	_, err := ParseCategory("bogus")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown category")
}

// Compile-time interface checks.
var (
	_ GeoScraper      = (*mockScraper)(nil)
	_ StateScraper    = (*mockStateScraper)(nil)
	_ AddressProducer = (*mockAddressProducer)(nil)
)

type mockAddressProducer struct{ mockScraper }

func (m *mockAddressProducer) HasAddresses() bool { return true }
