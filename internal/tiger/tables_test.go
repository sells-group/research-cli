package tiger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadURL_National(t *testing.T) {
	p, ok := ProductByName("STATE")
	require.True(t, ok)

	url := DownloadURL(p, 2024, "")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state_all.zip", url)
}

func TestDownloadURL_PerState(t *testing.T) {
	p, ok := ProductByName("EDGES")
	require.True(t, ok)

	url := DownloadURL(p, 2024, "12")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/EDGES/tl_2024_12_edges.zip", url)
}

func TestProductByName_Found(t *testing.T) {
	p, ok := ProductByName("ADDR")
	assert.True(t, ok)
	assert.Equal(t, "addr", p.Table)
	assert.False(t, p.National)
}

func TestProductByName_NotFound(t *testing.T) {
	_, ok := ProductByName("NONEXISTENT")
	assert.False(t, ok)
}

func TestFIPSCodes(t *testing.T) {
	// Spot-check a few states.
	assert.Equal(t, "12", FIPSCodes["FL"])
	assert.Equal(t, "06", FIPSCodes["CA"])
	assert.Equal(t, "36", FIPSCodes["NY"])
	assert.Equal(t, "48", FIPSCodes["TX"])
	assert.Equal(t, "11", FIPSCodes["DC"])
}

func TestAbbrFromFIPS(t *testing.T) {
	abbr, ok := AbbrFromFIPS("12")
	assert.True(t, ok)
	assert.Equal(t, "FL", abbr)

	_, ok = AbbrFromFIPS("99")
	assert.False(t, ok)
}

func TestAllStateFIPS(t *testing.T) {
	fips := AllStateFIPS()
	assert.True(t, len(fips) > 50) // 50 states + DC + territories
	// Should be sorted.
	for i := 1; i < len(fips); i++ {
		assert.True(t, fips[i-1] <= fips[i], "FIPS codes should be sorted")
	}
}

func TestAllStateAbbrs(t *testing.T) {
	abbrs := AllStateAbbrs()
	assert.True(t, len(abbrs) > 50)
	// Should be sorted.
	for i := 1; i < len(abbrs); i++ {
		assert.True(t, abbrs[i-1] <= abbrs[i], "abbreviations should be sorted")
	}
}

func TestNationalProducts(t *testing.T) {
	natl := NationalProducts()
	for _, p := range natl {
		assert.True(t, p.National, "product %s should be national", p.Name)
	}
	assert.True(t, len(natl) >= 5)
}

func TestPerStateProducts(t *testing.T) {
	perState := PerStateProducts()
	for _, p := range perState {
		assert.False(t, p.National, "product %s should be per-state", p.Name)
	}
	assert.True(t, len(perState) >= 4)
}

func TestProducts_HaveColumns(t *testing.T) {
	for _, p := range Products {
		assert.True(t, len(p.Columns) > 0, "product %s should have columns", p.Name)
	}
}
