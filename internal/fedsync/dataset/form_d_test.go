package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormD_Name(t *testing.T) {
	d := &FormD{}
	assert.Equal(t, "form_d", d.Name())
}

func TestFormD_Table(t *testing.T) {
	d := &FormD{}
	assert.Equal(t, "fed_data.form_d", d.Table())
}

func TestFormD_Phase(t *testing.T) {
	d := &FormD{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestFormD_Cadence(t *testing.T) {
	d := &FormD{}
	assert.Equal(t, Daily, d.Cadence())
}

func TestFormD_ShouldRun_NilLastSync(t *testing.T) {
	d := &FormD{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestFormD_ShouldRun_Today(t *testing.T) {
	d := &FormD{}
	now := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 15, 6, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestFormD_ShouldRun_Yesterday(t *testing.T) {
	d := &FormD{}
	now := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 14, 22, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestFormD_ShouldRun_LastWeek(t *testing.T) {
	d := &FormD{}
	now := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 8, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestFormD_ImplementsDataset(t *testing.T) {
	t.Parallel()
	var _ Dataset = &FormD{}
}
