package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOEWS_Metadata(t *testing.T) {
	ds := &OEWS{}
	assert.Equal(t, "oews", ds.Name())
	assert.Equal(t, "fed_data.oews_data", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestOEWS_ShouldRun(t *testing.T) {
	ds := &OEWS{}

	// Never synced -> should run
	now := time.Date(2024, time.May, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year -> should run (past April release)
	lastYear := time.Date(2023, time.July, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after release -> should not run
	thisYear := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date (March) -> should not run
	march := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(march, &lastYear))

	// Exactly April 1 -> past release, should run if last sync was before
	aprilFirst := time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(aprilFirst, &lastYear))

	// Synced in January, now is June -> should run
	janSync := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	june := time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(june, &janSync))
}
