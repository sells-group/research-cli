package scraper

// usgsSource is the source identifier for USGS/USGS-adjacent scrapers.
const usgsSource = "usgs"

// usgsBatchSize is the number of rows per BulkUpsert batch.
const usgsBatchSize = 5000

// padusBaseURL is the PAD-US (Protected Areas Database) FeatureServer endpoint.
const padusBaseURL = "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/PADUS_Fee_Easement/FeatureServer/0/query"

// oilGasWellsBaseURL is the Esri Living Atlas oil and gas wells FeatureServer endpoint.
const oilGasWellsBaseURL = "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/Oil_and_Natural_Gas_Wells/FeatureServer/0/query"

// nhdBaseURL is the USGS National Hydrography Dataset FeatureServer endpoint.
const nhdBaseURL = "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer/6/query"

// coalMinesBaseURL is the USGS coal mines FeatureServer endpoint.
const coalMinesBaseURL = "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/Coal_Mines/FeatureServer/0/query"

// usgsURL returns the base URL, falling back to the default endpoint
// if override is empty. The override is used for testing.
func usgsURL(override, defaultURL string) string {
	if override != "" {
		return override
	}
	return defaultURL
}
