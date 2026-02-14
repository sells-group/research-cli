package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFRED_Metadata(t *testing.T) {
	d := &FRED{}
	assert.Equal(t, "fred", d.Name())
	assert.Equal(t, "fed_data.fred_series", d.Table())
	assert.Equal(t, Phase3, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestFRED_ShouldRun(t *testing.T) {
	d := &FRED{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced this month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 3, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced last month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}
