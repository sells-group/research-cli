package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCBP_Metadata(t *testing.T) {
	ds := &CBP{}
	assert.Equal(t, "cbp", ds.Name())
	assert.Equal(t, "fed_data.cbp_data", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestCBP_ShouldRun(t *testing.T) {
	ds := &CBP{}

	// Never synced -> should run
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year -> should run (past March release)
	lastYear := time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after release -> should not run
	thisYear := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date -> should not run (even if synced long ago)
	beforeRelease := time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(beforeRelease, &lastYear))

	// Exactly on March 1 -> should run if last sync was before
	marchFirst := time.Date(2024, time.March, 2, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(marchFirst, &lastYear))

	// Synced January of this year, now is April -> should run (synced before March release)
	janSync := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &janSync))
}
