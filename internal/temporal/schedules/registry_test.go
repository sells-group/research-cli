package schedules

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllSchedules(t *testing.T) {
	schedules := AllSchedules()

	// We expect 7 schedules: 5 fedsync cadences + 2 geoscraper.
	require.Len(t, schedules, 7)

	// Verify all have required fields.
	ids := make(map[string]bool)
	for _, s := range schedules {
		require.NotEmpty(t, s.ID, "schedule ID must not be empty")
		require.NotEmpty(t, s.Cron, "schedule %s must have a cron expression", s.ID)
		require.NotEmpty(t, s.TaskQueue, "schedule %s must have a task queue", s.ID)
		require.NotNil(t, s.Workflow, "schedule %s must have a workflow", s.ID)
		require.NotEmpty(t, s.Description, "schedule %s must have a description", s.ID)
		require.False(t, ids[s.ID], "duplicate schedule ID: %s", s.ID)
		ids[s.ID] = true
	}

	// Spot-check specific schedules.
	require.True(t, ids["fedsync-daily"])
	require.True(t, ids["fedsync-weekly"])
	require.True(t, ids["fedsync-monthly"])
	require.True(t, ids["fedsync-quarterly"])
	require.True(t, ids["fedsync-annual"])
	require.True(t, ids["geo-national"])
	require.True(t, ids["geo-state"])
}

func TestStrPtr(t *testing.T) {
	p := strPtr("hello")
	require.NotNil(t, p)
	require.Equal(t, "hello", *p)
}
