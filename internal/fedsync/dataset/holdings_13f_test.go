package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHoldings13F_Name(t *testing.T) {
	d := &Holdings13F{}
	assert.Equal(t, "holdings_13f", d.Name())
}

func TestHoldings13F_Table(t *testing.T) {
	d := &Holdings13F{}
	assert.Equal(t, "fed_data.f13_holdings", d.Table())
}

func TestHoldings13F_Phase(t *testing.T) {
	d := &Holdings13F{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestHoldings13F_Cadence(t *testing.T) {
	d := &Holdings13F{}
	assert.Equal(t, Quarterly, d.Cadence())
}

func TestHoldings13F_ShouldRun_NilLastSync(t *testing.T) {
	d := &Holdings13F{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestHoldings13F_ShouldRun_QuarterlyAfterDelay(t *testing.T) {
	d := &Holdings13F{}

	// Q4 2024 ends Dec 31. Data available after 45 days = Feb 14.
	// Now is Feb 20, last sync was Jan 1 (before availability).
	now := time.Date(2025, 2, 20, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestHoldings13F_ShouldRun_TooEarly(t *testing.T) {
	d := &Holdings13F{}

	// Q4 2024 ends Dec 31. Data available after 45 days = Feb 14.
	// Now is Jan 15, too early for Q4 data. Q3 ended Sep 30, available Nov 14.
	// Last sync was Nov 20 (after Q3 availability).
	now := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2024, 11, 20, 0, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestHoldings13F_ShouldRun_AlreadySynced(t *testing.T) {
	d := &Holdings13F{}

	// Already synced after availability.
	now := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 2, 20, 0, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestHoldings13F_ImplementsDataset(t *testing.T) {
	var _ Dataset = &Holdings13F{}
}

func TestHoldings13F_SumHoldingsValue(t *testing.T) {
	d := &Holdings13F{}
	rows := [][]any{
		{nil, nil, nil, nil, nil, int64(1000), nil, nil, nil},
		{nil, nil, nil, nil, nil, int64(2000), nil, nil, nil},
		{nil, nil, nil, nil, nil, int64(500), nil, nil, nil},
	}
	assert.Equal(t, int64(3500), d.sumHoldingsValue(rows))
}

func TestHoldings13F_SumHoldingsValue_Empty(t *testing.T) {
	d := &Holdings13F{}
	assert.Equal(t, int64(0), d.sumHoldingsValue(nil))
}
