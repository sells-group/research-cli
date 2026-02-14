package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAnnualAfter(t *testing.T) {
	tests := []struct {
		name     string
		now      time.Time
		lastSync *time.Time
		month    time.Month
		expected bool
	}{
		{
			name:     "never synced",
			now:      time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC),
			lastSync: nil,
			month:    time.March,
			expected: true,
		},
		{
			name:     "synced last year, past release",
			now:      time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)),
			month:    time.March,
			expected: true,
		},
		{
			name:     "synced this year after release",
			now:      time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)),
			month:    time.March,
			expected: false,
		},
		{
			name:     "before release date",
			now:      time.Date(2024, time.February, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2023, time.March, 20, 0, 0, 0, 0, time.UTC)),
			month:    time.March,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AnnualAfter(tt.now, tt.lastSync, tt.month)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestMonthlySchedule(t *testing.T) {
	now := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)

	// Never synced
	assert.True(t, MonthlySchedule(now, nil))

	// Synced last month
	last := time.Date(2024, time.February, 20, 0, 0, 0, 0, time.UTC)
	assert.True(t, MonthlySchedule(now, &last))

	// Synced this month
	thisMonth := time.Date(2024, time.March, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, MonthlySchedule(now, &thisMonth))
}

func TestWeeklySchedule(t *testing.T) {
	// Wednesday March 13, 2024
	now := time.Date(2024, time.March, 13, 12, 0, 0, 0, time.UTC)

	// Never synced
	assert.True(t, WeeklySchedule(now, nil))

	// Synced last week
	lastWeek := time.Date(2024, time.March, 5, 0, 0, 0, 0, time.UTC)
	assert.True(t, WeeklySchedule(now, &lastWeek))

	// Synced this week (Monday)
	thisWeek := time.Date(2024, time.March, 11, 10, 0, 0, 0, time.UTC)
	assert.False(t, WeeklySchedule(now, &thisWeek))
}

func TestDailySchedule(t *testing.T) {
	now := time.Date(2024, time.March, 15, 14, 0, 0, 0, time.UTC)

	// Never synced
	assert.True(t, DailySchedule(now, nil))

	// Synced yesterday
	yesterday := time.Date(2024, time.March, 14, 10, 0, 0, 0, time.UTC)
	assert.True(t, DailySchedule(now, &yesterday))

	// Synced today
	today := time.Date(2024, time.March, 15, 2, 0, 0, 0, time.UTC)
	assert.False(t, DailySchedule(now, &today))
}

func TestQuarterlyWithLag(t *testing.T) {
	tests := []struct {
		name     string
		now      time.Time
		lastSync *time.Time
		lag      int
		expected bool
	}{
		{
			name:     "never synced",
			now:      time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC),
			lastSync: nil,
			lag:      5,
			expected: true,
		},
		{
			name:     "Q1 data available in June (5mo lag), last synced in Jan",
			now:      time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC)),
			lag:      5,
			expected: true,
		},
		{
			name:     "Q4 data available after 5mo lag in June",
			now:      time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC)),
			lag:      5,
			expected: true, // Q4 2023 (end Dec) + 5mo = May, now June → available
		},
		{
			name:     "Q1 data not yet available in May (5mo lag)",
			now:      time.Date(2024, time.May, 15, 0, 0, 0, 0, time.UTC),
			lastSync: ptr(time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC)),
			lag:      5,
			expected: false, // Q4 2023 end Dec + 5mo = May 31, not yet; Q3 end Sep + 5 = Feb, synced after
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuarterlyWithLag(tt.now, tt.lastSync, tt.lag)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestQuarterlyAfterDelay(t *testing.T) {
	// Never synced
	now := time.Date(2024, time.May, 20, 0, 0, 0, 0, time.UTC)
	assert.True(t, QuarterlyAfterDelay(now, nil, 45))

	// 45 days after Q1 end (March 31) = May 15. Now is May 20, last sync was April 1.
	lastSync := time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, QuarterlyAfterDelay(now, &lastSync, 45))

	// Already synced after availability
	recentSync := time.Date(2024, time.May, 16, 0, 0, 0, 0, time.UTC)
	assert.False(t, QuarterlyAfterDelay(now, &recentSync, 45))
}

func ptr(t time.Time) *time.Time {
	return &t
}
