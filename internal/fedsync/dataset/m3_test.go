package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestM3_Metadata(t *testing.T) {
	d := &M3{}
	assert.Equal(t, "m3", d.Name())
	assert.Equal(t, "fed_data.m3_data", d.Table())
	assert.Equal(t, Phase3, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestM3_ShouldRun(t *testing.T) {
	d := &M3{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced this month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 3, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced last month", func(t *testing.T) {
		now := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}

func TestParseTimeSlot(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		year, month := parseTimeSlot("2024-01")
		assert.Equal(t, 2024, year)
		assert.Equal(t, 1, month)
	})

	t.Run("valid december", func(t *testing.T) {
		year, month := parseTimeSlot("2023-12")
		assert.Equal(t, 2023, year)
		assert.Equal(t, 12, month)
	})

	t.Run("too short", func(t *testing.T) {
		year, month := parseTimeSlot("2024")
		assert.Equal(t, 0, year)
		assert.Equal(t, 0, month)
	})

	t.Run("wrong format", func(t *testing.T) {
		year, month := parseTimeSlot("2024/01")
		assert.Equal(t, 0, year)
		assert.Equal(t, 0, month)
	})

	t.Run("empty", func(t *testing.T) {
		year, month := parseTimeSlot("")
		assert.Equal(t, 0, year)
		assert.Equal(t, 0, month)
	})
}
