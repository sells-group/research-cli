package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBrokerCheck_Metadata(t *testing.T) {
	d := &BrokerCheck{}
	assert.Equal(t, "brokercheck", d.Name())
	assert.Equal(t, "fed_data.brokercheck", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestBrokerCheck_ShouldRun(t *testing.T) {
	d := &BrokerCheck{}

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
