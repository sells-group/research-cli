package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDegreesPerKM(t *testing.T) {
	// At ~45 degrees latitude, 1 degree ≈ 111 km.
	// So 2 km ≈ 0.018 degrees.
	cellKM := 2.0
	cellDeg := cellKM * DegreesPerKM

	assert.InDelta(t, 0.018, cellDeg, 0.001, "2 km should be approximately 0.018 degrees")

	// 10 km ≈ 0.09 degrees.
	cellKM = 10.0
	cellDeg = cellKM * DegreesPerKM
	assert.InDelta(t, 0.09, cellDeg, 0.005, "10 km should be approximately 0.09 degrees")
}

func TestDegreesPerKM_SmallValues(t *testing.T) {
	// 0.5 km ≈ 0.0045 degrees.
	cellDeg := 0.5 * DegreesPerKM
	assert.InDelta(t, 0.0045, cellDeg, 0.001)
}

func TestDegreesPerKM_LargeValues(t *testing.T) {
	// 100 km ≈ 0.9 degrees.
	cellDeg := 100.0 * DegreesPerKM
	assert.InDelta(t, 0.9, cellDeg, 0.01)
}
