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
	reg.Register(&HIFLDSchools{})
	reg.Register(&HIFLDFireEMS{})
	reg.Register(&HIFLDHospitals{})
	reg.Register(&HIFLDDams{})
	reg.Register(&HIFLDCemeteries{})
	reg.Register(&HIFLDHistoricPlaces{})
	reg.Register(&HIFLDRRCrossings{})
	reg.Register(&HIFLDAirports{})
	reg.Register(&HIFLDBridges{})
}

// RegisterFEMA registers all FEMA scrapers.
func RegisterFEMA(reg *geoscraper.Registry) {
	reg.Register(&FEMAFloodZones{})
	reg.Register(&FEMAFloodBulk{})
}

// RegisterEPA registers all EPA scrapers.
func RegisterEPA(reg *geoscraper.Registry) {
	reg.Register(&EPASites{})
	reg.Register(&EPAWastewater{})
	reg.Register(&EPABrownfields{})
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
	reg.Register(&TIGERBlockGroups{})
	reg.Register(&TIGERCousub{})
	reg.Register(&TIGERWater{})
}

// RegisterUSGS registers all USGS/USGS-adjacent scrapers.
func RegisterUSGS(reg *geoscraper.Registry) {
	reg.Register(&USGSProtectedAreas{})
	reg.Register(&USGSOilGasWells{})
	reg.Register(&USGSWaterways{})
	reg.Register(&USGSCoalMines{})
	reg.Register(&USGSEarthquakes{})
}

// RegisterBulkGDB registers GDB-based bulk scrapers that replace ArcGIS equivalents.
func RegisterBulkGDB(reg *geoscraper.Registry) {
	reg.Register(&NHDWaterwaysBulk{})
	reg.Register(&PADUSProtectedAreasBulk{})
}

// RegisterOSM registers all OpenStreetMap scrapers.
func RegisterOSM(reg *geoscraper.Registry) {
	reg.Register(&OSMPOI{})
}

// RegisterBulkCSV registers CSV-based scrapers that replace ArcGIS equivalents.
func RegisterBulkCSV(reg *geoscraper.Registry, cfg *config.Config) {
	var nrelKey string
	if cfg != nil {
		nrelKey = cfg.Fedsync.NRELKey
	}
	reg.Register(&FHWABridges{})
	reg.Register(&FAAAirports{})
	reg.Register(&USACEDams{})
	reg.Register(&FRARRCrossings{})
	reg.Register(&AFDCEVCharging{apiKey: nrelKey})
}

// RegisterNTAD registers all NTAD/DOT transportation scrapers.
func RegisterNTAD(reg *geoscraper.Registry) {
	reg.Register(&NTADPorts{})
	reg.Register(&BTSAmtrakStations{})
	reg.Register(&BTSFreightRail{})
	reg.Register(&FHWAHPMs{})
	reg.Register(&NTADFerryTerminals{})
	reg.Register(&NTADIntercityBus{})
	reg.Register(&NTADTransitStations{})
}

// RegisterEIA registers all EIA scrapers.
func RegisterEIA(reg *geoscraper.Registry) {
	reg.Register(&EIAPlants{})
}

// RegisterCDC registers all CDC scrapers.
func RegisterCDC(reg *geoscraper.Registry) {
	reg.Register(&CDCSvi{})
}

// RegisterFDICGeo registers FDIC geo scrapers.
func RegisterFDICGeo(reg *geoscraper.Registry) {
	reg.Register(&FDICBranches{})
}

// RegisterHUD registers all HUD housing scrapers.
func RegisterHUD(reg *geoscraper.Registry) {
	reg.Register(&HUDLihtc{})
	reg.Register(&HUDFMR{})
}

// RegisterEPASLD registers the EPA Smart Location Database scraper.
func RegisterEPASLD(reg *geoscraper.Registry) {
	reg.Register(&EPASmartLocation{})
}

// RegisterImports registers all cross-database import scrapers.
func RegisterImports(reg *geoscraper.Registry) {
	reg.Register(&ImportPPP{})
	reg.Register(&ImportCBP{})
	reg.Register(&ImportQCEW{})
	reg.Register(&ImportEPA{})
	reg.Register(&GeocodePPP{})
}

// RegisterBLM registers all BLM scrapers.
func RegisterBLM(reg *geoscraper.Registry) {
	reg.Register(&BLMFederalLands{})
	reg.Register(&BLMMineralLeases{})
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
	RegisterUSGS(reg)
	RegisterTIGER(reg)
	RegisterOSM(reg)
	RegisterBulkCSV(reg, cfg)
	RegisterNTAD(reg)
	RegisterEIA(reg)
	RegisterCDC(reg)
	RegisterFDICGeo(reg)
	RegisterHUD(reg)
	RegisterEPASLD(reg)
	RegisterImports(reg)
	RegisterBulkGDB(reg)
	RegisterBLM(reg)
}
