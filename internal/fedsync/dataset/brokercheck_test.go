package dataset

import (
	"context"
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

	t.Run("always returns false (disabled)", func(t *testing.T) {
		assert.False(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("returns false even with old lastSync", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})
}

func TestBrokerCheck_Sync_Disabled(t *testing.T) {
	d := &BrokerCheck{}
	_, err := d.Sync(context.Background(), nil, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disabled")
}
