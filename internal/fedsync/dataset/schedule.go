package dataset

import "time"

// AnnualAfter returns true if a sync is needed for an annual dataset
// that releases after the given month. Syncs once per year after the release month.
func AnnualAfter(now time.Time, lastSync *time.Time, releaseMonth time.Month) bool {
	if lastSync == nil {
		return true
	}
	// Release date for the current year.
	releaseDate := time.Date(now.Year(), releaseMonth, 1, 0, 0, 0, 0, time.UTC)
	// Only sync if we're past the release date and haven't synced since it.
	return now.After(releaseDate) && lastSync.Before(releaseDate)
}

// QuarterlyWithLag returns true if a sync is needed for a quarterly dataset
// with the given lag in months after quarter end.
func QuarterlyWithLag(now time.Time, lastSync *time.Time, lagMonths int) bool {
	if lastSync == nil {
		return true
	}
	// Find the most recent quarter-end that's old enough (past the lag).
	qEnd := mostRecentQuarterEnd(now)
	available := qEnd.AddDate(0, lagMonths, 0)
	if now.Before(available) {
		// Data for this quarter isn't available yet; check previous quarter.
		qEnd = mostRecentQuarterEnd(qEnd.AddDate(0, 0, -1))
		available = qEnd.AddDate(0, lagMonths, 0)
		if now.Before(available) {
			return false
		}
	}
	return lastSync.Before(available)
}

// MonthlySchedule returns true if a sync is needed for a monthly dataset.
func MonthlySchedule(now time.Time, lastSync *time.Time) bool {
	if lastSync == nil {
		return true
	}
	thisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	return lastSync.Before(thisMonth)
}

// WeeklySchedule returns true if a sync is needed for a weekly dataset.
func WeeklySchedule(now time.Time, lastSync *time.Time) bool {
	if lastSync == nil {
		return true
	}
	// Find the start of the current ISO week (Monday).
	weekday := int(now.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	weekStart := time.Date(now.Year(), now.Month(), now.Day()-(weekday-1), 0, 0, 0, 0, time.UTC)
	return lastSync.Before(weekStart)
}

// DailySchedule returns true if a sync is needed for a daily dataset.
func DailySchedule(now time.Time, lastSync *time.Time) bool {
	if lastSync == nil {
		return true
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return lastSync.Before(today)
}

// QuarterlyAfterDelay returns true if a sync is needed for a quarterly dataset
// that becomes available a certain number of days after the quarter ends.
func QuarterlyAfterDelay(now time.Time, lastSync *time.Time, delayDays int) bool {
	if lastSync == nil {
		return true
	}
	qEnd := mostRecentQuarterEnd(now)
	available := qEnd.AddDate(0, 0, delayDays)
	if now.Before(available) {
		qEnd = mostRecentQuarterEnd(qEnd.AddDate(0, 0, -1))
		available = qEnd.AddDate(0, 0, delayDays)
		if now.Before(available) {
			return false
		}
	}
	return lastSync.Before(available)
}

// mostRecentQuarterEnd returns the last day of the most recent completed quarter.
func mostRecentQuarterEnd(t time.Time) time.Time {
	year := t.Year()
	month := t.Month()

	var qEndMonth time.Month
	var qEndYear int

	switch {
	case month >= time.January && month <= time.March:
		// We're in Q1 — last completed quarter is Q4 of previous year.
		qEndMonth = time.December
		qEndYear = year - 1
	case month >= time.April && month <= time.June:
		qEndMonth = time.March
		qEndYear = year
	case month >= time.July && month <= time.September:
		qEndMonth = time.June
		qEndYear = year
	default: // Oct-Dec
		qEndMonth = time.September
		qEndYear = year
	}

	// Last day of qEndMonth.
	return time.Date(qEndYear, qEndMonth+1, 0, 23, 59, 59, 0, time.UTC)
}
