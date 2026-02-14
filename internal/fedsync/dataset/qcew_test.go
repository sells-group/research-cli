package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQCEW_Metadata(t *testing.T) {
	ds := &QCEW{}
	assert.Equal(t, "qcew", ds.Name())
	assert.Equal(t, "fed_data.qcew_data", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Quarterly, ds.Cadence())
}

func TestQCEW_ShouldRun(t *testing.T) {
	ds := &QCEW{}

	// Never synced -> should run
	now := time.Date(2024, time.September, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// 5-month lag: Q1 (Jan-Mar) data available ~August
	// Now is September, last synced in July -> Q1 data now available
	julSync := time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &julSync))

	// Now is September, synced in September -> should not run (already got this quarter's data)
	sepSync := time.Date(2024, time.September, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &sepSync))

	// Now is June, Q4 data (Oct-Dec 2023) + 5 months = May 2024 -> available
	june := time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC)
	lastOct := time.Date(2023, time.October, 1, 0, 0, 0, 0, time.UTC)
	// Q4 2023 ends Dec 31, + 5 months = May 31, synced Oct -> should run
	assert.True(t, ds.ShouldRun(june, &lastOct))

	// Now is February, too early for any new quarter
	feb := time.Date(2024, time.February, 15, 0, 0, 0, 0, time.UTC)
	janSync := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	// Q3 2023 (Jul-Sep) + 5 = Feb... borderline case
	// Q2 2023 (Apr-Jun) + 5 = Nov 2023, synced Jan -> should not run again
	assert.False(t, ds.ShouldRun(feb, &janSync))
}

func TestQCEW_IsRelevantFile(t *testing.T) {
	ds := &QCEW{}

	assert.True(t, ds.isRelevantFile("2023.q1-q4 52 NAICS 52.csv"))
	assert.True(t, ds.isRelevantFile("2023.q1-q4 54 NAICS 54.csv"))
	assert.True(t, ds.isRelevantFile("path/to/10 total all.csv"))
	assert.False(t, ds.isRelevantFile("2023.q1-q4 31 NAICS 31.csv"))
	assert.False(t, ds.isRelevantFile("readme.txt"))
}
