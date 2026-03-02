// Package geo provides spatial operations for MSA association and urban classification.
package geo

// Urban classification constants.
const (
	ClassUrbanCore = "urban_core"
	ClassSuburban  = "suburban"
	ClassExurban   = "exurban"
	ClassRural     = "rural"
)

// Distance thresholds for classification (kilometers).
const (
	urbanCoreCentroidThreshold = 8.0  // within MSA AND centroid distance <= 8km
	exurbanEdgeThreshold       = 40.0 // outside MSA AND edge distance <= 40km
)

// Classify returns the urban classification for a point relative to an MSA.
// Rules:
//   - urban_core: within MSA AND centroid distance <= 8km
//   - suburban: within MSA AND centroid distance > 8km
//   - exurban: outside MSA AND edge distance <= 40km
//   - rural: outside MSA AND edge distance > 40km
func Classify(isWithin bool, centroidKM, edgeKM float64) string {
	if isWithin {
		if centroidKM <= urbanCoreCentroidThreshold {
			return ClassUrbanCore
		}
		return ClassSuburban
	}
	if edgeKM <= exurbanEdgeThreshold {
		return ClassExurban
	}
	return ClassRural
}
