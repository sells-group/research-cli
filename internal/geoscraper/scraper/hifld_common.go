// Package scraper contains geo scraper implementations for HIFLD, FEMA, EPA,
// and other national/state-level data sources.
package scraper

import (
	"encoding/json"
	"time"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// hifldSource is the source identifier for all HIFLD scrapers.
const hifldSource = "hifld"

// hifldURL returns the base URL, falling back to the default ArcGIS endpoint
// for the given layer if override is empty. The override is used for testing.
func hifldURL(override, layer string) string {
	if override != "" {
		return override
	}
	return arcgis.FormatURL(layer)
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
