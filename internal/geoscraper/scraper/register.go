package scraper

import "github.com/sells-group/research-cli/internal/geoscraper"

// RegisterHIFLD registers all HIFLD infrastructure scrapers.
func RegisterHIFLD(reg *geoscraper.Registry) {
	reg.Register(&HIFLDPowerPlants{})
	reg.Register(&HIFLDSubstations{})
	reg.Register(&HIFLDTransmissionLines{})
	reg.Register(&HIFLDPipelines{})
}

// RegisterFEMA registers all FEMA scrapers.
func RegisterFEMA(reg *geoscraper.Registry) {
	reg.Register(&FEMAFloodZones{})
}

// RegisterEPA registers all EPA scrapers.
func RegisterEPA(reg *geoscraper.Registry) {
	reg.Register(&EPASites{})
}

// RegisterAll registers all geo scraper implementations.
func RegisterAll(reg *geoscraper.Registry) {
	RegisterHIFLD(reg)
	RegisterFEMA(reg)
	RegisterEPA(reg)
}
