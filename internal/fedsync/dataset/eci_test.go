package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestECI_Metadata(t *testing.T) {
	d := &ECI{}
	assert.Equal(t, "eci", d.Name())
	assert.Equal(t, "fed_data.eci_data", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Quarterly, d.Cadence())
}

func TestECI_ShouldRun(t *testing.T) {
	d := &ECI{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("recently synced within quarter", func(t *testing.T) {
		// Q1 ends March 31; with 2 month lag, available June 1.
		// If we're in June and synced in June, don't run.
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 2, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("new quarter data available", func(t *testing.T) {
		// Q1 ends March 31; with 2 month lag, available June 1.
		// If we're in June and last synced in April, should run.
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}
