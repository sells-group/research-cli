package schedules

import (
	"testing"

	"github.com/stretchr/testify/require"

	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
)

func TestAllSchedules(t *testing.T) {
	schedules := AllSchedules()

	require.Len(t, schedules, 3)

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
	require.True(t, ids["geo-national"])
	require.True(t, ids["geo-state"])
}

func TestFedsyncSchedules_LaunchAllDueDatasets(t *testing.T) {
	schedules := FedsyncSchedules()
	require.Len(t, schedules, 1)

	params, ok := schedules[0].Args[0].(temporalfedsync.RunParams)
	require.True(t, ok)
	require.Nil(t, params.Phase)
	require.Empty(t, params.Datasets)
}

func TestGeoSchedules_LaunchExpectedCategories(t *testing.T) {
	schedules := GeoSchedules()
	require.Len(t, schedules, 2)

	params, ok := schedules[0].Args[0].(temporalgeoscraper.ScrapeParams)
	require.True(t, ok)
	require.NotNil(t, params.Category)
	require.Equal(t, "national", *params.Category)

	params, ok = schedules[1].Args[0].(temporalgeoscraper.ScrapeParams)
	require.True(t, ok)
	require.NotNil(t, params.Category)
	require.Equal(t, "state", *params.Category)
}

func TestStrPtr(t *testing.T) {
	p := strPtr("hello")
	require.NotNil(t, p)
	require.Equal(t, "hello", *p)
}
