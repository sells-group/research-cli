package schedules

import (
	enumspb "go.temporal.io/api/enums/v1"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
)

// FedsyncSchedules returns the canonical Temporal schedules for fedsync.
func FedsyncSchedules() []Schedule {
	return []Schedule{
		{
			ID:          "fedsync-daily",
			Description: "Fedsync daily sweep for all due datasets",
			Cron:        "0 2 * * *",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "daily"},
		},
	}
}

// GeoSchedules returns the Temporal schedules for the geoscraper domain.
func GeoSchedules() []Schedule {
	return []Schedule{
		{
			ID:          "geo-national",
			Description: "National geo scrapers",
			Cron:        "0 7 * * *",
			TaskQueue:   temporalpkg.GeoTaskQueue,
			Workflow:    temporalgeoscraper.ScrapeWorkflow,
			Args: []interface{}{temporalgeoscraper.ScrapeParams{
				Category: strPtr("national"),
			}},
			Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:    map[string]string{"domain": "geoscraper", "cadence": "daily"},
		},
		{
			ID:          "geo-state",
			Description: "State geo scrapers (weekly Monday)",
			Cron:        "0 8 * * 1",
			TaskQueue:   temporalpkg.GeoTaskQueue,
			Workflow:    temporalgeoscraper.ScrapeWorkflow,
			Args: []interface{}{temporalgeoscraper.ScrapeParams{
				Category: strPtr("state"),
			}},
			Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:    map[string]string{"domain": "geoscraper", "cadence": "weekly"},
		},
	}
}

// AllSchedules returns every schedule the system needs.
func AllSchedules() []Schedule {
	schedules := make([]Schedule, 0, len(FedsyncSchedules())+len(GeoSchedules()))
	schedules = append(schedules, FedsyncSchedules()...)
	schedules = append(schedules, GeoSchedules()...)
	return schedules
}

func strPtr(s string) *string { return &s }
