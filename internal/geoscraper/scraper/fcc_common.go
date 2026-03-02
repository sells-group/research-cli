package scraper

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"

	"github.com/sells-group/research-cli/internal/tiger"
)

// fccSource is the source identifier for all FCC scrapers.
const fccSource = "fcc"

// fccBatchSize is the number of rows per BulkUpsert batch.
const fccBatchSize = 5000

// fccTowerURL is the HIFLD Cellular Towers shapefile download URL.
const fccTowerURL = "https://opendata.arcgis.com/api/v3/datasets/0835ba2ed38f494196c14571f3758eb0_0/downloads/data?format=shp&spatialRefId=4326"

// fccTowerProduct defines the shapefile columns to extract for cellular towers.
var fccTowerProduct = tiger.Product{
	Name:     "FCC_TOWERS",
	Columns:  []string{"objectid", "licensee", "locid", "strucheigh"},
	GeomType: "POINT",
}

// fccTechMap maps FCC BDC technology codes to readable names.
var fccTechMap = map[string]string{
	"10": "dsl",
	"40": "cable",
	"50": "fiber",
	"60": "satellite",
	"70": "fixed_wireless",
	"71": "fixed_wireless",
	"72": "fixed_wireless",
}

// fccTechName returns the readable technology name for a code.
func fccTechName(code string) string {
	if name, ok := fccTechMap[code]; ok {
		return name
	}
	return "other"
}

// broadbandCols are the columns written to geo.broadband_coverage.
var broadbandCols = []string{
	"block_geoid", "technology", "max_download", "max_upload",
	"provider_count", "latitude", "longitude",
	"source", "source_id", "properties",
}

// broadbandConflictKeys defines the unique constraint columns for broadband upserts.
var broadbandConflictKeys = []string{"source", "source_id"}

// newTowerRow builds a row for the geo.infrastructure table from a parsed
// shapefile row. Returns nil, false if the row has no valid geometry.
// Input shpRow: [objectid, licensee, locid, strucheigh, wkb_geom_bytes].
func newTowerRow(shpRow []any) ([]any, bool) {
	if len(shpRow) < 5 {
		return nil, false
	}
	wkb, ok := shpRow[4].([]byte)
	if !ok || wkb == nil {
		return nil, false
	}
	g, err := ewkb.Unmarshal(wkb)
	if err != nil {
		return nil, false
	}
	pt, ok := g.(*geom.Point)
	if !ok {
		return nil, false
	}
	lon, lat := pt.X(), pt.Y()

	objectID := fmt.Sprintf("%v", shpRow[0])
	licensee := ""
	if s, ok := shpRow[1].(string); ok {
		licensee = s
	}
	height := 0.0
	if s, ok := shpRow[3].(string); ok {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			height = f
		}
	}

	sourceID := fmt.Sprintf("fcc_tower/%s", objectID)
	props, _ := json.Marshal(map[string]any{
		"locid":         shpRow[2],
		"struct_height": shpRow[3],
	})

	return []any{
		licensee,        // name
		"telecom_tower", // type
		nil,             // fuel_type
		height,          // capacity (tower height)
		lat, lon,
		fccSource,
		sourceID,
		props,
	}, true
}
