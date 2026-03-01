package scraper

import (
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

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

// RegisterCensus registers all Census ACS scrapers.
func RegisterCensus(reg *geoscraper.Registry, cfg *config.Config) {
	var apiKey string
	if cfg != nil {
		apiKey = cfg.Fedsync.CensusKey
	}
	reg.Register(&CensusDemographics{apiKey: apiKey})
}

// RegisterAll registers all geo scraper implementations.
func RegisterAll(reg *geoscraper.Registry, cfg *config.Config) {
	RegisterHIFLD(reg)
	RegisterFEMA(reg)
	RegisterEPA(reg)
	RegisterCensus(reg, cfg)
}
