package scraper

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sells-group/research-cli/internal/tiger"
)

// nwiSource is the source identifier for all NWI scrapers.
const nwiSource = "nwi"

// nwiBatchSize is the number of rows per BulkUpsert batch.
const nwiBatchSize = 5000

// nwiDownloadURL returns the FWS NWI shapefile download URL for a state.
func nwiDownloadURL(baseURL, state string) string {
	if baseURL != "" {
		return baseURL + "/" + state + "_shapefile_wetlands.zip"
	}
	return fmt.Sprintf(
		"https://www.fws.gov/wetlands/Data/State-Downloads/%s_shapefile_wetlands.zip",
		state,
	)
}

// nwiProduct defines the shapefile columns for NWI wetland data.
// DBF fields: OBJECTID, ATTRIBUTE (Cowardin code), WETLAND_TY (type), ACRES.
var nwiProduct = tiger.Product{
	Name:     "NWI_WETLANDS",
	Columns:  []string{"objectid", "attribute", "wetland_ty", "acres"},
	GeomType: "MULTIPOLYGON",
}

// wetlandCols are the columns written to geo.wetlands by the NWI scraper.
var wetlandCols = []string{
	"wetland_type", "attribute", "acres",
	"geom", "source", "source_id", "properties",
}

// wetlandConflictKeys defines the unique constraint columns for upserts.
var wetlandConflictKeys = []string{"source", "source_id"}

// classifyWetland maps a Cowardin classification code prefix to a wetland type.
func classifyWetland(attribute string) string {
	if attribute == "" {
		return "unknown"
	}
	switch strings.ToUpper(attribute[:1]) {
	case "E":
		return "estuarine"
	case "M":
		return "marine"
	case "P":
		return "palustrine"
	case "L":
		return "lacustrine"
	case "R":
		return "riverine"
	default:
		return "unknown"
	}
}

// newWetlandRow builds a row for geo.wetlands from a ParseShapefile row.
// Input row: [objectid, attribute, wetland_ty, acres, wkb_bytes].
// Returns nil, false if geometry is missing.
func newWetlandRow(shpRow []any) ([]any, bool) {
	if len(shpRow) < 5 {
		return nil, false
	}
	wkb, ok := shpRow[4].([]byte)
	if !ok || wkb == nil {
		return nil, false
	}

	objectID := ""
	if s, ok := shpRow[0].(string); ok {
		objectID = s
	}

	attribute := ""
	if s, ok := shpRow[1].(string); ok {
		attribute = s
	}

	wetlandType := classifyWetland(attribute)

	var acres *float64
	if s, ok := shpRow[3].(string); ok && s != "" {
		var f float64
		if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
			acres = &f
		}
	}

	sourceID := fmt.Sprintf("nwi/%s", objectID)

	wetlandTy := ""
	if s, ok := shpRow[2].(string); ok {
		wetlandTy = s
	}

	props, _ := json.Marshal(map[string]any{
		"wetland_ty": wetlandTy,
	})

	return []any{
		wetlandType,
		attribute,
		acres,
		wkb,
		nwiSource,
		sourceID,
		props,
	}, true
}
