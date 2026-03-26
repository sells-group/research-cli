// Package schedules provides declarative Temporal schedule management.
package schedules

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// Schedule defines a declarative Temporal schedule.
type Schedule struct {
	ID          string                        // "fedsync-cbp"
	Description string                        // human-readable description
	Cron        string                        // "0 2 * * *" (standard cron)
	TaskQueue   string                        // "fedsync"
	Workflow    interface{}                   // workflow function ref
	Args        []interface{}                 // workflow params
	Overlap     enumspb.ScheduleOverlapPolicy // SKIP (default)
	Paused      bool                          // start paused?
	Tags        map[string]string             // {"domain": "fedsync", "source": "census"}
}
