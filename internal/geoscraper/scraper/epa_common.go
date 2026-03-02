package scraper

import "fmt"

// epaSource is the source identifier for all EPA FRS scrapers.
const epaSource = "epa_frs"

// epaBatchSize is the number of rows per upsert batch.
const epaBatchSize = 5000

// epaBaseURL is the EPA Facility Registry Service ArcGIS MapServer endpoint.
const epaBaseURL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer/0/query"

// epaURL returns the base URL, falling back to the default EPA endpoint
// if override is empty. The override is used for testing.
func epaURL(override string) string {
	if override != "" {
		return override
	}
	return epaBaseURL
}

// epaCols are the columns written to geo.epa_sites by the EPA scraper.
var epaCols = []string{
	"name", "program", "registry_id", "status",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

// epaConflictKeys defines the unique constraint columns for upserts.
var epaConflictKeys = []string{"source", "source_id"}

// epaExclude lists attribute keys stored in dedicated columns.
var epaExclude = map[string]bool{
	"OBJECTID":      true,
	"REGISTRY_ID":   true,
	"PRIMARY_NAME":  true,
	"PGM_SYS_ACRNM": true,
	"ACTIVE_STATUS": true,
	"LATITUDE83":    true,
	"LONGITUDE83":   true,
}

// stateAbbrevs lists all 50 US states + DC + territories by postal abbreviation.
var stateAbbrevs = []string{
	"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
	"GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
	"MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
	"NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
	"SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
	"WY",
	// Territories
	"AS", "GU", "MP", "PR", "VI",
}

// buildEPAWhere returns an ArcGIS WHERE clause to filter by state abbreviation.
func buildEPAWhere(stateCode string) string {
	return fmt.Sprintf("STATE_CODE='%s'", stateCode)
}
