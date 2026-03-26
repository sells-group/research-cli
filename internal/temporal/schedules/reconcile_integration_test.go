//go:build integration

package schedules

import (
	"context"
	"encoding/json"
	"os/exec"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
)

func TestReconcileIntegration_CreateNoopUpdatePruneAndArgs(t *testing.T) {
	ctx := context.Background()
	c := newIntegrationClient(t, ctx)

	desired := AllSchedules()

	result, err := Reconcile(ctx, c, desired, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fedsync-daily", "geo-national", "geo-state"}, result.Created)

	ids := listScheduleIDs(t, ctx, c)
	assert.ElementsMatch(t, []string{"fedsync-daily", "geo-national", "geo-state"}, ids)

	result, err = Reconcile(ctx, c, desired, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fedsync-daily", "geo-national", "geo-state"}, result.Unchanged)
	assert.Empty(t, result.Created)
	assert.Empty(t, result.Updated)
	assert.Empty(t, result.Deleted)

	modified := slices.Clone(desired)
	modified[0].Cron = "0 3 * * *"
	result, err = Reconcile(ctx, c, modified, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"fedsync-daily"}, result.Updated)

	result, err = Reconcile(ctx, c, desired, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"fedsync-daily"}, result.Updated)

	require.NoError(t, createLegacyFedsyncSchedule(ctx, c))
	result, err = Reconcile(ctx, c, desired, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	assert.Contains(t, result.Deleted, "fedsync-phase1")

	handle := c.ScheduleClient().GetHandle(ctx, "fedsync-daily")
	desc, err := handle.Describe(ctx)
	require.NoError(t, err)

	action, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	require.Len(t, action.Args, 1)

	var params temporalfedsync.RunParams
	payload, err := json.Marshal(action.Args[0])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &params))
	assert.Nil(t, params.Phase)
	assert.Empty(t, params.Datasets)
	assert.False(t, params.Force)
	assert.False(t, params.Full)
}

func newIntegrationClient(t *testing.T, ctx context.Context) client.Client {
	t.Helper()

	path, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI not found in PATH")
	}

	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: path,
		ClientOptions: &client.Options{
			Namespace: "default",
		},
		LogLevel:  "error",
		LogFormat: "json",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dev.Stop())
	})

	return dev.Client()
}

func listScheduleIDs(t *testing.T, ctx context.Context, c client.Client) []string {
	t.Helper()

	iter, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	require.NoError(t, err)

	var ids []string
	for iter.HasNext() {
		entry, err := iter.Next()
		require.NoError(t, err)
		ids = append(ids, entry.ID)
	}
	return ids
}

func createLegacyFedsyncSchedule(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: "fedsync-phase1",
		Spec: client.ScheduleSpec{
			CronExpressions: []string{"0 1 * * *"},
			TimeZoneName:    "UTC",
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        "legacy-fedsync-phase1",
			Workflow:  temporalfedsync.RunWorkflow,
			TaskQueue: temporalpkg.FedsyncTaskQueue,
			Args: []interface{}{temporalfedsync.RunParams{
				Phase: strPtr("1"),
			}},
		},
	})
	return err
}
