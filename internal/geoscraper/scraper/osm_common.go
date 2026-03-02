package scraper

import (
	"encoding/json"
	"fmt"

	"github.com/sells-group/research-cli/internal/geoscraper/overpass"
)

// osmSource is the source identifier for all OSM scrapers.
const osmSource = "osm"

// osmBatchSize is the number of rows per BulkUpsert batch.
const osmBatchSize = 5000

// poiCols are the columns written to geo.poi by OSM scrapers.
var poiCols = []string{
	"name", "category", "subcategory",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

// poiConflictKeys defines the unique constraint columns for upserts.
var poiConflictKeys = []string{"source", "source_id"}

// usBBox defines the CONUS bounding box: [south, west, north, east].
var usBBox = [4]float64{24.396308, -125.0, 49.384358, -66.93457}

// bboxTile represents a geographic bounding box tile.
type bboxTile struct {
	south, west, north, east float64
}

// usTiles splits CONUS into a grid of bounding box tiles to avoid
// Overpass API timeouts on large queries.
func usTiles() []bboxTile {
	const latStep = 5.0  // ~5 degrees latitude
	const lonStep = 12.0 // ~12 degrees longitude

	var tiles []bboxTile
	for lat := usBBox[0]; lat < usBBox[2]; lat += latStep {
		for lon := usBBox[1]; lon < usBBox[3]; lon += lonStep {
			north := min(lat+latStep, usBBox[2])
			east := min(lon+lonStep, usBBox[3])
			tiles = append(tiles, bboxTile{lat, lon, north, east})
		}
	}
	return tiles
}

// categorizeOSM maps OSM tags to POI category and subcategory.
func categorizeOSM(tags map[string]string) (category, subcategory string) {
	if v, ok := tags["amenity"]; ok {
		switch v {
		case "school":
			return "education", "school"
		case "hospital":
			return "healthcare", "hospital"
		case "fire_station":
			return "emergency", "fire_station"
		case "police":
			return "emergency", "police"
		case "library":
			return "education", "library"
		case "post_office":
			return "government", "post_office"
		case "place_of_worship":
			return "religious", "place_of_worship"
		default:
			return "other", v
		}
	}
	if v, ok := tags["leisure"]; ok {
		if v == "park" {
			return "recreation", "park"
		}
		return "recreation", v
	}
	return "other", "unknown"
}

// newPOIRow builds a row for the geo.poi table from an Overpass element.
// Returns nil, false if the element has no name tag.
func newPOIRow(elem overpass.Element) ([]any, bool) {
	name := elem.Tags["name"]
	if name == "" {
		return nil, false
	}

	category, subcategory := categorizeOSM(elem.Tags)
	sourceID := fmt.Sprintf("osm/%d", elem.ID)

	// Build properties from all tags except name.
	props := make(map[string]any)
	for k, v := range elem.Tags {
		if k != "name" {
			props[k] = v
		}
	}
	propsJSON, _ := json.Marshal(props)

	return []any{
		name,
		category,
		subcategory,
		elem.Lat,
		elem.Lon,
		osmSource,
		sourceID,
		propsJSON,
	}, true
}
