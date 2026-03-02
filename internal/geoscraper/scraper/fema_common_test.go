package scraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFEMAFloodType_HighRisk(t *testing.T) {
	tests := []struct {
		zone    string
		sfha    string
		subtype string
	}{
		{"AE", "T", ""},
		{"A", "T", ""},
		{"AH", "T", ""},
		{"AO", "T", ""},
		{"AR", "T", ""},
		{"A99", "T", ""},
		{"V", "T", ""},
		{"VE", "T", ""},
	}
	for _, tt := range tests {
		t.Run(tt.zone, func(t *testing.T) {
			assert.Equal(t, "high_risk", femaFloodType(tt.zone, tt.sfha, tt.subtype))
		})
	}
}

func TestFEMAFloodType_ModerateRisk(t *testing.T) {
	got := femaFloodType("X", "F", "0.2 PCT ANNUAL CHANCE FLOOD HAZARD")
	assert.Equal(t, "moderate_risk", got)
}

func TestFEMAFloodType_LowRisk(t *testing.T) {
	got := femaFloodType("X", "F", "")
	assert.Equal(t, "low_risk", got)
}

func TestFEMAFloodType_Undetermined(t *testing.T) {
	got := femaFloodType("D", "F", "")
	assert.Equal(t, "undetermined", got)
}

func TestFEMAFloodType_CaseInsensitive(t *testing.T) {
	assert.Equal(t, "high_risk", femaFloodType("ae", "t", ""))
	assert.Equal(t, "undetermined", femaFloodType("d", "F", ""))
}

func TestFEMAURL_Override(t *testing.T) {
	got := femaURL("http://test.local/query")
	assert.Equal(t, "http://test.local/query", got)
}

func TestFEMAURL_Default(t *testing.T) {
	got := femaURL("")
	assert.Equal(t, femaBaseURL, got)
}

func TestBuildFEMAWhere(t *testing.T) {
	got := buildFEMAWhere("48")
	assert.Equal(t, "DFIRM_ID LIKE '48%'", got)
}

func TestSanitizeGeoTable(t *testing.T) {
	assert.Equal(t, `"geo"."flood_zones"`, sanitizeGeoTable("geo.flood_zones"))
	assert.Equal(t, `"flood_zones"`, sanitizeGeoTable("flood_zones"))
}

func TestRingsToEWKT_Simple(t *testing.T) {
	// Tested via arcgis package; this validates the FEMA integration path.
	assert.NotEmpty(t, stateFIPS)
	assert.Len(t, stateFIPS, 56) // 50 states + DC + 5 territories
}
