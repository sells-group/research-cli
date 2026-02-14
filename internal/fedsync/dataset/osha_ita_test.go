package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOSHITA_Metadata(t *testing.T) {
	d := &OSHITA{}
	assert.Equal(t, "osha_ita", d.Name())
	assert.Equal(t, "fed_data.osha_inspections", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Annual, d.Cadence())
}

func TestOSHITA_ShouldRun(t *testing.T) {
	d := &OSHITA{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("before release month", func(t *testing.T) {
		now := time.Date(2025, 2, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("after release month, not synced this year", func(t *testing.T) {
		now := time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})

	t.Run("after release month, already synced", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})
}
