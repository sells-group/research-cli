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

// RegisterFCC registers all FCC scrapers.
func RegisterFCC(reg *geoscraper.Registry, cfg *config.Config) {
	reg.Register(&FCCTowers{})
	var bdcKey string
	if cfg != nil {
		bdcKey = cfg.Fedsync.FCCBDCKey
	}
	reg.Register(&FCCBroadband{apiKey: bdcKey})
}

// RegisterNWI registers all NWI scrapers.
func RegisterNWI(reg *geoscraper.Registry) {
	reg.Register(&NWIWetlands{})
}

// RegisterNRCS registers all NRCS scrapers.
func RegisterNRCS(reg *geoscraper.Registry) {
	reg.Register(&NRCSSoils{})
}

// RegisterTIGER registers all TIGER/Line scrapers.
func RegisterTIGER(reg *geoscraper.Registry) {
	reg.Register(&TIGERBoundaries{})
	reg.Register(&TIGERRoads{})
}

// RegisterOSM registers all OpenStreetMap scrapers.
func RegisterOSM(reg *geoscraper.Registry) {
	reg.Register(&OSMPOI{})
}

// RegisterAll registers all geo scraper implementations.
func RegisterAll(reg *geoscraper.Registry, cfg *config.Config) {
	RegisterHIFLD(reg)
	RegisterFEMA(reg)
	RegisterEPA(reg)
	RegisterCensus(reg, cfg)
	RegisterFCC(reg, cfg)
	RegisterNWI(reg)
	RegisterNRCS(reg)
	RegisterTIGER(reg)
	RegisterOSM(reg)
}
