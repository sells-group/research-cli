package scraper

import "github.com/sells-group/research-cli/internal/geoscraper"

// RegisterHIFLD registers all HIFLD infrastructure scrapers.
func RegisterHIFLD(reg *geoscraper.Registry) {
	reg.Register(&HIFLDPowerPlants{})
	reg.Register(&HIFLDSubstations{})
	reg.Register(&HIFLDTransmissionLines{})
	reg.Register(&HIFLDPipelines{})
}

// RegisterAll registers all geo scraper implementations.
// Future additions (FEMA, EPA, etc.) should add their own Register* calls here.
func RegisterAll(reg *geoscraper.Registry) {
	RegisterHIFLD(reg)
}
