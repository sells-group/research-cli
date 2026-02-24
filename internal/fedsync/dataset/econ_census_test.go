package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEconCensus_Metadata(t *testing.T) {
	ds := &EconCensus{}
	assert.Equal(t, "econ_census", ds.Name())
	assert.Equal(t, "fed_data.economic_census", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Annual, ds.Cadence())
}

func TestEconCensus_ShouldRun(t *testing.T) {
	ds := &EconCensus{}

	// Never synced -> should run (backfill)
	now := time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced recently in 2024, no new census year -> should not run
	recentSync := time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &recentSync))

	// Before March -> should not run
	jan := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
	oldSync := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(jan, &oldSync))

	// 2022 census data released ~2024; synced in 2023 -> should run in 2024
	sync2023 := time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	now2024 := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now2024, &sync2023))

	// Synced in 2024 (after 2022 census release) -> should not run again in 2024
	sync2024 := time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now2024, &sync2024))
}

func TestEconCensus_ParseResponse(t *testing.T) {
	ds := &EconCensus{}

	data := []byte(`[
		["GEO_ID","NAICS2017","ESTAB","RCPTOT","PAYANN","EMP","state"],
		["0400000US06","523110","1500","5000000","2000000","15000","06"],
		["0400000US36","312100","800","3000000","1000000","8000","36"],
		["0400000US48","541100","2200","7000000","3500000","22000","48"]
	]`)

	rows, err := ds.parseResponse(data, 2022)
	assert.NoError(t, err)
	// All NAICS codes are accepted
	assert.Len(t, rows, 3)

	// Check first row (523110)
	assert.Equal(t, int16(2022), rows[0][0])
	assert.Equal(t, "0400000US06", rows[0][1]) // geo_id
	assert.Equal(t, "523110", rows[0][2])      // naics
	assert.Equal(t, 1500, rows[0][3])          // estab

	// Check second row (312100)
	assert.Equal(t, "312100", rows[1][2])

	// Check third row (541100)
	assert.Equal(t, "541100", rows[2][2])
}

func TestEconCensus_ParseResponse_NAICS2022(t *testing.T) {
	ds := &EconCensus{}

	// 2022 Census API returns NAICS2022 instead of NAICS2017
	data := []byte(`[
		["GEO_ID","NAICS2022","ESTAB","RCPTOT","PAYANN","EMP","state"],
		["0400000US06","523110","1500","5000000","2000000","15000","06"],
		["0400000US36","312100","800","3000000","1000000","8000","36"]
	]`)

	rows, err := ds.parseResponse(data, 2022)
	assert.NoError(t, err)
	assert.Len(t, rows, 2)

	assert.Equal(t, int16(2022), rows[0][0])
	assert.Equal(t, "523110", rows[0][2])
	assert.Equal(t, "312100", rows[1][2])
}

func TestEconCensus_ParseResponse_Empty(t *testing.T) {
	ds := &EconCensus{}

	// Only header, no data
	data := []byte(`[["GEO_ID","NAICS2017","ESTAB","RCPTOT","PAYANN","EMP","state"]]`)
	rows, err := ds.parseResponse(data, 2022)
	assert.NoError(t, err)
	assert.Empty(t, rows)
}

func TestEconCensus_ParseResponse_InvalidJSON(t *testing.T) {
	ds := &EconCensus{}
	_, err := ds.parseResponse([]byte(`not json`), 2022)
	assert.Error(t, err)
}
