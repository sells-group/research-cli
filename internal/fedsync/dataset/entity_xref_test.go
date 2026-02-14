package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntityXref_Name(t *testing.T) {
	d := &EntityXref{}
	assert.Equal(t, "entity_xref", d.Name())
}

func TestEntityXref_Table(t *testing.T) {
	d := &EntityXref{}
	assert.Equal(t, "fed_data.entity_xref", d.Table())
}

func TestEntityXref_Phase(t *testing.T) {
	d := &EntityXref{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestEntityXref_Cadence(t *testing.T) {
	d := &EntityXref{}
	assert.Equal(t, Monthly, d.Cadence())
}

func TestEntityXref_ShouldRun_NilLastSync(t *testing.T) {
	d := &EntityXref{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestEntityXref_ShouldRun_AlwaysTrue(t *testing.T) {
	d := &EntityXref{}
	// Should always return true regardless of lastSync.
	now := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)

	lastSync := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))

	lastSync = time.Date(2025, 3, 14, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))

	lastSync = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestEntityXref_ImplementsDataset(t *testing.T) {
	var _ Dataset = &EntityXref{}
}
