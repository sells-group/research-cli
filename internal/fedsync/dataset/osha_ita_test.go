package dataset

import (
	"context"
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

	t.Run("always returns false (disabled)", func(t *testing.T) {
		assert.False(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("returns false even with old lastSync", func(t *testing.T) {
		now := time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})
}

func TestOSHITA_Sync_Disabled(t *testing.T) {
	d := &OSHITA{}
	_, err := d.Sync(context.Background(), nil, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disabled")
}
