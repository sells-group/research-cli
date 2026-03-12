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
	require.Len(t, names, 13)
	assert.Equal(t, "hifld_power_plants", names[0])
	assert.Equal(t, "hifld_substations", names[1])
	assert.Equal(t, "hifld_transmission_lines", names[2])
	assert.Equal(t, "hifld_pipelines", names[3])
	assert.Equal(t, "hifld_schools", names[4])
	assert.Equal(t, "hifld_fire_ems", names[5])
	assert.Equal(t, "hifld_hospitals", names[6])
	assert.Equal(t, "hifld_dams", names[7])
	assert.Equal(t, "hifld_cemeteries", names[8])
	assert.Equal(t, "hifld_historic_places", names[9])
	assert.Equal(t, "hifld_rr_crossings", names[10])
	assert.Equal(t, "hifld_airports", names[11])
	assert.Equal(t, "hifld_bridges", names[12])
}

func TestRegisterAll(t *testing.T) {
	reg := geoscraper.NewRegistry()
	RegisterAll(reg, nil)

	names := reg.AllNames()
	require.Len(t, names, 45) // 13 HIFLD + 2 FEMA + 3 EPA + 1 Census + 2 FCC + 1 NWI + 1 NRCS + 4 USGS + 5 TIGER + 1 OSM + 5 BulkCSV + 4 NTAD + 1 EIA + 1 CDC + 1 FDIC

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
	require.Len(t, names, 45)
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
	_ geoscraper.GeoScraper = (*HIFLDSchools)(nil)
	_ geoscraper.GeoScraper = (*HIFLDFireEMS)(nil)
	_ geoscraper.GeoScraper = (*HIFLDHospitals)(nil)
	_ geoscraper.GeoScraper = (*HIFLDDams)(nil)
	_ geoscraper.GeoScraper = (*HIFLDCemeteries)(nil)
	_ geoscraper.GeoScraper = (*HIFLDHistoricPlaces)(nil)
	_ geoscraper.GeoScraper = (*HIFLDRRCrossings)(nil)
	_ geoscraper.GeoScraper = (*HIFLDAirports)(nil)
	_ geoscraper.GeoScraper = (*HIFLDBridges)(nil)
	_ geoscraper.GeoScraper = (*USGSProtectedAreas)(nil)
	_ geoscraper.GeoScraper = (*USGSOilGasWells)(nil)
	_ geoscraper.GeoScraper = (*USGSWaterways)(nil)
	_ geoscraper.GeoScraper = (*USGSCoalMines)(nil)
	_ geoscraper.GeoScraper = (*EPAWastewater)(nil)
	_ geoscraper.GeoScraper = (*EPABrownfields)(nil)
	_ geoscraper.GeoScraper = (*FHWABridges)(nil)
	_ geoscraper.GeoScraper = (*FAAAirports)(nil)
	_ geoscraper.GeoScraper = (*USACEDams)(nil)
	_ geoscraper.GeoScraper = (*FRARRCrossings)(nil)
	_ geoscraper.GeoScraper = (*AFDCEVCharging)(nil)
	_ geoscraper.GeoScraper = (*NTADPorts)(nil)
	_ geoscraper.GeoScraper = (*BTSAmtrakStations)(nil)
	_ geoscraper.GeoScraper = (*BTSFreightRail)(nil)
	_ geoscraper.GeoScraper = (*FHWAHPMs)(nil)
	_ geoscraper.GeoScraper = (*TIGERBlockGroups)(nil)
	_ geoscraper.GeoScraper = (*TIGERCousub)(nil)
	_ geoscraper.GeoScraper = (*TIGERWater)(nil)
	_ geoscraper.GeoScraper = (*FEMAFloodBulk)(nil)
	_ geoscraper.GeoScraper = (*EIAPlants)(nil)
	_ geoscraper.GeoScraper = (*CDCSvi)(nil)
	_ geoscraper.GeoScraper = (*FDICBranches)(nil)
)
