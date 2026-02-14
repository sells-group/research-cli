package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestXBRLFacts_Metadata(t *testing.T) {
	d := &XBRLFacts{}
	assert.Equal(t, "xbrl_facts", d.Name())
	assert.Equal(t, "fed_data.xbrl_facts", d.Table())
	assert.Equal(t, Phase3, d.Phase())
	assert.Equal(t, Daily, d.Cadence())
}

func TestXBRLFacts_ShouldRun(t *testing.T) {
	d := &XBRLFacts{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced today", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 14, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 15, 8, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced yesterday", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 14, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 14, 20, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}
