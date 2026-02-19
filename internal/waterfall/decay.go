package waterfall

import (
	"math"
	"time"
)

// EffectiveConfidence computes the time-decayed confidence of a data point.
// Formula: effective = max(floor, rawConfidence * 2^(-ageDays / halfLifeDays))
func EffectiveConfidence(rawConfidence float64, dataAsOf time.Time, now time.Time, decay DecayConfig) float64 {
	if rawConfidence <= 0 {
		return 0
	}
	if dataAsOf.IsZero() {
		// No timestamp — use raw confidence as-is (assume current).
		return rawConfidence
	}

	ageDays := now.Sub(dataAsOf).Hours() / 24
	if ageDays <= 0 {
		return rawConfidence
	}

	halfLife := float64(decay.HalfLifeDays)
	if halfLife <= 0 {
		halfLife = 365 // safe default
	}

	decayed := rawConfidence * math.Pow(2, -ageDays/halfLife)

	if decayed < decay.Floor {
		return decay.Floor
	}
	return decayed
}
