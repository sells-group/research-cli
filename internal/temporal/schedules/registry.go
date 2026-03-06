package schedules

import (
	enumspb "go.temporal.io/api/enums/v1"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
)

// AllSchedules returns every schedule the system needs.
// Individual datasets get their own schedules with appropriate cron expressions
// matching their upstream cadence. ShouldRun() + AvailabilityChecker inside the
// workflow still gate actual execution — the cron just controls check frequency.
func AllSchedules() []Schedule {
	return []Schedule{
		// --- Fedsync ---
		{
			ID:          "fedsync-daily",
			Description: "Fedsync daily datasets (FPDS, FormD, IACompilation, XBRLFacts)",
			Cron:        "0 2 * * *",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "daily"},
		},
		{
			ID:          "fedsync-weekly",
			Description: "Fedsync weekly datasets (EDGARSubmissions, ADVPart1, FDICBankFind)",
			Cron:        "0 3 * * 1",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "weekly"},
		},
		{
			ID:          "fedsync-monthly",
			Description: "Fedsync monthly datasets (EOBMF, BrokerCheck, EPAECHO, FRED, etc.)",
			Cron:        "0 4 1 * *",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "monthly"},
		},
		{
			ID:          "fedsync-quarterly",
			Description: "Fedsync quarterly datasets (QCEW, SBA, Holdings13F, ECI)",
			Cron:        "0 5 1 1,4,7,10 *",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "quarterly"},
		},
		{
			ID:          "fedsync-annual",
			Description: "Fedsync annual datasets (CBP, SUSB, OEWS, Form5500, EconCensus, etc.)",
			Cron:        "0 6 1 3 *",
			TaskQueue:   temporalpkg.FedsyncTaskQueue,
			Workflow:    temporalfedsync.RunWorkflow,
			Args:        []interface{}{temporalfedsync.RunParams{}},
			Overlap:     enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Tags:        map[string]string{"domain": "fedsync", "cadence": "annual"},
		},

		// --- Geoscraper ---
		{
			ID:          "geo-national",
			Description: "National geo scrapers (HIFLD, FEMA, EPA, Census) — availability gated",
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

func strPtr(s string) *string { return &s }
