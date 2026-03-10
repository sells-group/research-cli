// Package scraper contains geo scraper implementations for HIFLD, FEMA, EPA,
// and other national/state-level data sources.
package scraper

import (
	"encoding/json"
	"time"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

// hifldSource is the source identifier for all HIFLD scrapers.
const hifldSource = "hifld"

// Per-dataset ArcGIS FeatureServer/MapServer base URLs.
// The old HIFLD org (Hp6G80Pky0om6HgA) is dead; each dataset is now hosted
// on a different ArcGIS server.
const (
	schoolsBaseURL     = "https://services1.arcgis.com/Ua5sjt3LWTPigjyD/arcgis/rest/services/Public_School_Locations_Current/FeatureServer/0/query"
	fireEMSBaseURL     = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Structures_Medical_Emergency_Response_v1/FeatureServer/2/query"
	hospitalsBaseURL   = "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/Hospitals_hifld/FeatureServer/0/query"
	damsBaseURL        = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/NID_v1/FeatureServer/0/query"
	cemeteriesBaseURL  = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer/2/query"
	historicPlacesURL  = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/nrhp_points_v1/FeatureServer/0/query"
	rrCrossingsBaseURL = "https://services1.arcgis.com/4yjifSiIG17X0gW4/arcgis/rest/services/FRA_Crossing_Inventory_Form_71_Current/FeatureServer/0/query"
	airportsBaseURL    = "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/US_Airport/FeatureServer/0/query"
	bridgesBaseURL     = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Bridge_Inventory/FeatureServer/0/query"
)

// hifldURL returns the override URL if set, otherwise the default.
func hifldURL(override, defaultURL string) string {
	if override != "" {
		return override
	}
	return defaultURL
}

// hifldBatchSize is the number of rows per BulkUpsert batch.
const hifldBatchSize = 5000

// infraCols are the columns written to geo.infrastructure by all HIFLD scrapers.
var infraCols = []string{
	"name", "type", "fuel_type", "capacity",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

// infraConflictKeys defines the unique constraint columns for upserts.
var infraConflictKeys = []string{"source", "source_id"}

// hifldString safely extracts a string from ArcGIS attributes.
func hifldString(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// hifldFloat64 safely extracts a float64 from ArcGIS attributes,
// handling both float64 and json.Number values.
func hifldFloat64(attrs map[string]any, key string) float64 {
	v, ok := attrs[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// hifldProperties builds a JSONB-compatible byte slice from attributes,
// excluding the specified keys (which are stored in dedicated columns).
func hifldProperties(attrs map[string]any, exclude map[string]bool) []byte {
	props := make(map[string]any)
	for k, v := range attrs {
		if exclude[k] || v == nil {
			continue
		}
		props[k] = v
	}
	data, err := json.Marshal(props)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// hifldShouldRun returns true if a quarterly HIFLD scraper is due.
func hifldShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.QuarterlyAfterDelay(now, lastSync, 0)
}

// hifldAnnualShouldRun returns true if an annual HIFLD scraper is due.
func hifldAnnualShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}
