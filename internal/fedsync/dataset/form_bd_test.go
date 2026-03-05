package dataset

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormBD_Metadata(t *testing.T) {
	d := &FormBD{}
	assert.Equal(t, "form_bd", d.Name())
	assert.Equal(t, "fed_data.form_bd", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestFormBD_ShouldRun(t *testing.T) {
	d := &FormBD{}

	t.Run("always returns false (disabled)", func(t *testing.T) {
		assert.False(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("returns false even with old lastSync", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 10, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})
}

func TestFormBD_Sync_Disabled(t *testing.T) {
	d := &FormBD{}
	_, err := d.Sync(context.Background(), nil, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disabled")
}
