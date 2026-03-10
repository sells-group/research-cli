package scraper

// usgsSource is the source identifier for USGS/USGS-adjacent scrapers.
const usgsSource = "usgs"

// usgsBatchSize is the number of rows per BulkUpsert batch.
const usgsBatchSize = 5000

// padusBaseURL is the PAD-US (Protected Areas Database) FeatureServer endpoint.
const padusBaseURL = "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services/Fee_Managers_PADUS/FeatureServer/0/query"

// oilGasWellsBaseURL is the USGS documented orphaned oil/gas wells FeatureServer endpoint.
const oilGasWellsBaseURL = "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services/US_Documented_Unplugged_Orphaned_Oil_and_Gas_Well_Dataset/FeatureServer/5/query"

// nhdBaseURL is the USGS National Hydrography Dataset MapServer endpoint.
const nhdBaseURL = "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer/6/query"

// coalMinesBaseURL is the EIA/MSHA coal mines FeatureServer endpoint.
const coalMinesBaseURL = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Surface_and_Underground_Coal_Mines_in_the_US/FeatureServer/0/query"

// usgsURL returns the base URL, falling back to the default endpoint
// if override is empty. The override is used for testing.
func usgsURL(override, defaultURL string) string {
	if override != "" {
		return override
	}
	return defaultURL
}
