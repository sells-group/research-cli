package scraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestRegisterHIFLD(t *testing.T) {
	reg := geoscraper.NewRegistry()
	RegisterHIFLD(reg)

	names := reg.AllNames()
	require.Len(t, names, 4)
	assert.Equal(t, "hifld_power_plants", names[0])
	assert.Equal(t, "hifld_substations", names[1])
	assert.Equal(t, "hifld_transmission_lines", names[2])
	assert.Equal(t, "hifld_pipelines", names[3])
}

func TestRegisterAll(t *testing.T) {
	reg := geoscraper.NewRegistry()
	RegisterAll(reg, nil)

	names := reg.AllNames()
	require.Len(t, names, 14) // 4 HIFLD + 1 FEMA + 1 EPA + 1 Census + 2 FCC + 1 NWI + 1 NRCS + 2 TIGER + 1 OSM

	// All should be National category.
	for _, s := range reg.All() {
		assert.Equal(t, geoscraper.National, s.Category())
	}
}

func TestRegisterAll_WithConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.Fedsync.CensusKey = "test-census-key"
	cfg.Fedsync.FCCBDCKey = "test-fcc-key"

	reg := geoscraper.NewRegistry()
	RegisterAll(reg, cfg)

	names := reg.AllNames()
	require.Len(t, names, 14)
}

func TestRegisterAll_NoDuplicates(t *testing.T) {
	reg := geoscraper.NewRegistry()
	RegisterAll(reg, nil)

	seen := make(map[string]bool)
	for _, name := range reg.AllNames() {
		assert.False(t, seen[name], "duplicate scraper name: %s", name)
		seen[name] = true
	}
}

// Compile-time interface checks.
var (
	_ geoscraper.GeoScraper = (*HIFLDPowerPlants)(nil)
	_ geoscraper.GeoScraper = (*HIFLDSubstations)(nil)
	_ geoscraper.GeoScraper = (*HIFLDTransmissionLines)(nil)
	_ geoscraper.GeoScraper = (*HIFLDPipelines)(nil)
	_ geoscraper.GeoScraper = (*FEMAFloodZones)(nil)
	_ geoscraper.GeoScraper = (*EPASites)(nil)
	_ geoscraper.GeoScraper = (*CensusDemographics)(nil)
	_ geoscraper.GeoScraper = (*FCCTowers)(nil)
	_ geoscraper.GeoScraper = (*FCCBroadband)(nil)
	_ geoscraper.GeoScraper = (*NWIWetlands)(nil)
	_ geoscraper.GeoScraper = (*NRCSSoils)(nil)
	_ geoscraper.GeoScraper = (*TIGERBoundaries)(nil)
	_ geoscraper.GeoScraper = (*TIGERRoads)(nil)
	_ geoscraper.GeoScraper = (*OSMPOI)(nil)
)
