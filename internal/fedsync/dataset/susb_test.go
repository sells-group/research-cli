package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSUSB_Metadata(t *testing.T) {
	ds := &SUSB{}
	assert.Equal(t, "susb", ds.Name())
	assert.Equal(t, "fed_data.susb_data", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestSUSB_ShouldRun(t *testing.T) {
	ds := &SUSB{}

	// Never synced -> should run
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced last year -> should run (past March release)
	lastYear := time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastYear))

	// Synced this year after release -> should not run
	thisYear := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisYear))

	// Before release date -> should not run
	beforeRelease := time.Date(2024, time.February, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(beforeRelease, &lastYear))

	// Synced in February, now is May -> should run
	febSync := time.Date(2024, time.February, 10, 0, 0, 0, 0, time.UTC)
	may := time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(may, &febSync))
}
