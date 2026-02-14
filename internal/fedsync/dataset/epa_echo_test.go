package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEPAECHO_Metadata(t *testing.T) {
	d := &EPAECHO{}
	assert.Equal(t, "epa_echo", d.Name())
	assert.Equal(t, "fed_data.epa_facilities", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestEPAECHO_ShouldRun(t *testing.T) {
	d := &EPAECHO{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced this month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 5, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced last month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}
