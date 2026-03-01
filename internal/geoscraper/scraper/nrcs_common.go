package scraper

import (
	"encoding/json"
	"fmt"

	"github.com/sells-group/research-cli/internal/tiger"
)

// nrcsSource is the source identifier for all NRCS scrapers.
const nrcsSource = "nrcs"

// nrcsBatchSize is the number of rows per BulkUpsert batch.
const nrcsBatchSize = 5000

// nrcsDefaultURL is the NRCS gSSURGO national soils shapefile download URL.
const nrcsDefaultURL = "https://nrcs.app.box.com/v/soils/file/gsmsoilmu_a_us.zip"

// nrcsURL returns the download URL, falling back to the default NRCS endpoint
// if override is empty. The override is used for testing.
func nrcsURL(override string) string {
	if override != "" {
		return override
	}
	return nrcsDefaultURL
}

// nrcsProduct defines the shapefile columns for NRCS soil map unit data.
// DBF fields: OBJECTID, MUKEY, MUNAME, DRCLASSDCD, HYDRICRATI.
var nrcsProduct = tiger.Product{
	Name:     "NRCS_SOILS",
	Columns:  []string{"objectid", "mukey", "muname", "drclassdcd", "hydricrati"},
	GeomType: "MULTIPOLYGON",
}

// soilCols are the columns written to geo.soils by the NRCS scraper.
var soilCols = []string{
	"mukey", "muname", "drainage_class", "hydric_rating",
	"geom", "source", "source_id", "properties",
}

// soilConflictKeys defines the unique constraint columns for upserts.
var soilConflictKeys = []string{"source", "source_id"}

// newSoilRow builds a row for geo.soils from a ParseShapefile row.
// Input row: [objectid, mukey, muname, drclassdcd, hydricrati, wkb_bytes].
// Returns nil, false if geometry is missing or mukey is empty.
func newSoilRow(shpRow []any) ([]any, bool) {
	if len(shpRow) < 6 {
		return nil, false
	}
	wkb, ok := shpRow[5].([]byte)
	if !ok || wkb == nil {
		return nil, false
	}

	mukey := ""
	if s, ok := shpRow[1].(string); ok {
		mukey = s
	}
	if mukey == "" {
		return nil, false
	}

	muname := ""
	if s, ok := shpRow[2].(string); ok {
		muname = s
	}

	drclassdcd := ""
	if s, ok := shpRow[3].(string); ok {
		drclassdcd = s
	}

	hydricRating := ""
	if s, ok := shpRow[4].(string); ok {
		hydricRating = s
	}

	sourceID := fmt.Sprintf("nrcs/%s", mukey)

	objectID := ""
	if s, ok := shpRow[0].(string); ok {
		objectID = s
	}

	props, _ := json.Marshal(map[string]any{
		"objectid": objectID,
	})

	return []any{
		mukey,
		muname,
		drclassdcd,
		hydricRating,
		wkb,
		nrcsSource,
		sourceID,
		props,
	}, true
}
