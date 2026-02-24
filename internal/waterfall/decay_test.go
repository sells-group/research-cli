package waterfall

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEffectiveConfidence_Current(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	// Data from today — no decay.
	got := EffectiveConfidence(0.9, now, now, decay)
	assert.Equal(t, 0.9, got)
}

func TestEffectiveConfidence_HalfLife(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oneYearAgo := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	// Data exactly one half-life old → confidence halved.
	got := EffectiveConfidence(0.8, oneYearAgo, now, decay)
	assert.InDelta(t, 0.4, got, 0.02)
}

func TestEffectiveConfidence_TwoHalfLives(t *testing.T) {
	now := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	twoYearsAgo := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.1}

	// Two half-lives → confidence quartered.
	got := EffectiveConfidence(0.8, twoYearsAgo, now, decay)
	assert.InDelta(t, 0.2, got, 0.02)
}

func TestEffectiveConfidence_Floor(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ancient := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.15}

	// Very old data — should hit floor.
	got := EffectiveConfidence(0.9, ancient, now, decay)
	assert.Equal(t, 0.15, got)
}

func TestEffectiveConfidence_ZeroConfidence(t *testing.T) {
	now := time.Now()
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	got := EffectiveConfidence(0.0, now, now, decay)
	assert.Equal(t, 0.0, got)
}

func TestEffectiveConfidence_NegativeConfidence(t *testing.T) {
	now := time.Now()
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	got := EffectiveConfidence(-0.5, now, now, decay)
	assert.Equal(t, 0.0, got)
}

func TestEffectiveConfidence_ZeroDataAsOf(t *testing.T) {
	now := time.Now()
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	// Zero time means "assume current" — no decay.
	got := EffectiveConfidence(0.8, time.Time{}, now, decay)
	assert.Equal(t, 0.8, got)
}

func TestEffectiveConfidence_FutureDataAsOf(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	future := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 365, Floor: 0.2}

	// Data from the future — no decay.
	got := EffectiveConfidence(0.8, future, now, decay)
	assert.Equal(t, 0.8, got)
}

func TestEffectiveConfidence_ZeroHalfLife(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oneYearAgo := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 0, Floor: 0.1} // should default to 365

	got := EffectiveConfidence(0.8, oneYearAgo, now, decay)
	// Should use default 365 half-life.
	assert.InDelta(t, 0.4, got, 0.02)
}

func TestEffectiveConfidence_LongHalfLife(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fiveYearsAgo := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 1825, Floor: 0.3} // 5-year half-life

	// Exactly one half-life → halved.
	got := EffectiveConfidence(0.9, fiveYearsAgo, now, decay)
	assert.InDelta(t, 0.45, got, 0.02)
}

func TestEffectiveConfidence_DecayCurve(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	decay := DecayConfig{HalfLifeDays: 180, Floor: 0.15}

	// Test multiple points on the curve.
	tests := []struct {
		name       string
		daysBefore int
		raw        float64
		expected   float64
	}{
		{"30d", 30, 0.8, 0.8 * math.Pow(2, -30.0/180)},
		{"90d", 90, 0.8, 0.8 * math.Pow(2, -90.0/180)},
		{"180d", 180, 0.8, 0.4},
		{"360d", 360, 0.8, 0.2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataAsOf := now.AddDate(0, 0, -tc.daysBefore)
			got := EffectiveConfidence(tc.raw, dataAsOf, now, decay)
			expected := tc.expected
			if expected < decay.Floor {
				expected = decay.Floor
			}
			assert.InDelta(t, expected, got, 0.02)
		})
	}
}
