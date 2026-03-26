package schedules

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

func TestReconcile_CreateNew(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(false)
	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return(&mocks.ScheduleHandle{}, nil)

	desired := []Schedule{
		{ID: "test-1", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	result, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{"test-1"}, result.Created)
	require.Empty(t, result.Updated)
	require.Empty(t, result.Deleted)
	require.Empty(t, result.Unchanged)
}

func TestReconcile_UpdateExisting(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	desired := Schedule{ID: "test-1", Cron: "0 3 * * *", TaskQueue: "q", Workflow: "wf"}
	existingDesc := &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{ID: "legacy-action"},
			State: &client.ScheduleState{
				Paused: true,
				Note:   "paused by operator",
			},
		},
	}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: desired.ID}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, desired.ID).Return(mockHandle)
	mockHandle.On("Describe", mock.Anything).Return(existingDesc, nil)
	mockHandle.On("Update", mock.Anything, mock.MatchedBy(func(opts client.ScheduleUpdateOptions) bool {
		update, err := opts.DoUpdate(client.ScheduleUpdateInput{Description: *existingDesc})
		require.NoError(t, err)
		require.NotNil(t, update)
		require.NotNil(t, update.Schedule)
		require.NotNil(t, update.Schedule.State)
		require.True(t, update.Schedule.State.Paused)
		require.Equal(t, "paused by operator", update.Schedule.State.Note)

		action, ok := update.Schedule.Action.(*client.ScheduleWorkflowAction)
		require.True(t, ok)
		require.Equal(t, desiredActionID(desired), action.ID)
		require.Equal(t, scheduleFingerprint(desired), action.Memo[scheduleFingerprintMemoKey])

		return true
	})).Return(nil)

	result, err := Reconcile(context.Background(), mockClient, []Schedule{desired}, ReconcileOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{desired.ID}, result.Updated)
	require.Empty(t, result.Created)
	require.Empty(t, result.Deleted)
}

func TestReconcile_UnchangedExisting(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	desired := Schedule{ID: "test-1", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"}
	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: desired.ID}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, desired.ID).Return(mockHandle)
	mockHandle.On("Describe", mock.Anything).Return(describedSchedule(desired), nil)

	result, err := Reconcile(context.Background(), mockClient, []Schedule{desired}, ReconcileOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{desired.ID}, result.Unchanged)
	require.Empty(t, result.Created)
	require.Empty(t, result.Updated)
	require.Empty(t, result.Deleted)
	mockHandle.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestReconcile_PruneUnmanaged(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "orphan-1"}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, "orphan-1").Return(mockHandle)
	mockHandle.On("Delete", mock.Anything).Return(nil)

	result, err := Reconcile(context.Background(), mockClient, nil, ReconcileOpts{Prune: true})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan-1"}, result.Deleted)
}

func TestReconcile_UnmanagedWithoutPrune(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "orphan-1"}, nil)

	result, err := Reconcile(context.Background(), mockClient, nil, ReconcileOpts{Prune: false})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan-1"}, result.Unchanged)
	require.Empty(t, result.Deleted)
}

func TestReconcile_DryRun(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	existing := Schedule{ID: "existing", Cron: "0 3 * * *", TaskQueue: "q", Workflow: "wf"}
	newSchedule := Schedule{ID: "new-one", Cron: "0 4 * * *", TaskQueue: "q", Workflow: "wf"}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: existing.ID}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, existing.ID).Return(mockHandle)
	mockHandle.On("Describe", mock.Anything).Return(&client.ScheduleDescription{
		Schedule: client.Schedule{Action: &client.ScheduleWorkflowAction{ID: "outdated"}},
	}, nil)

	result, err := Reconcile(context.Background(), mockClient, []Schedule{existing, newSchedule}, ReconcileOpts{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, []string{newSchedule.ID}, result.Created)
	require.Equal(t, []string{existing.ID}, result.Updated)
	mockScheduleClient.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
	mockHandle.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestReconcile_ListError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return((*mocks.ScheduleListIterator)(nil), errTest)

	_, err := Reconcile(context.Background(), mockClient, nil, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "list existing")
}

func TestReconcile_IterateError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true)
	mockIter.On("Next").Return((*client.ScheduleListEntry)(nil), errTest)

	_, err := Reconcile(context.Background(), mockClient, nil, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "iterate")
}

func TestReconcile_CreateError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(false)
	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return((*mocks.ScheduleHandle)(nil), errTest)

	desired := []Schedule{
		{ID: "fail", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	_, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create fail")
}

func TestReconcile_DescribeError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	desired := Schedule{ID: "existing", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: desired.ID}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, desired.ID).Return(mockHandle)
	mockHandle.On("Describe", mock.Anything).Return((*client.ScheduleDescription)(nil), errTest)

	_, err := Reconcile(context.Background(), mockClient, []Schedule{desired}, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "describe existing")
}

func TestReconcile_UpdateError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	desired := Schedule{ID: "existing", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: desired.ID}, nil)
	mockScheduleClient.On("GetHandle", mock.Anything, desired.ID).Return(mockHandle)
	mockHandle.On("Describe", mock.Anything).Return(&client.ScheduleDescription{
		Schedule: client.Schedule{Action: &client.ScheduleWorkflowAction{ID: "legacy-action"}},
	}, nil)
	mockHandle.On("Update", mock.Anything, mock.Anything).Return(errTest)

	_, err := Reconcile(context.Background(), mockClient, []Schedule{desired}, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "update existing")
}

func TestReconcile_DryRunPrune(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "orphan-1"}, nil)

	result, err := Reconcile(context.Background(), mockClient, nil, ReconcileOpts{DryRun: true, Prune: true})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan-1"}, result.Deleted)
	mockScheduleClient.AssertNotCalled(t, "GetHandle", mock.Anything, mock.Anything)
}

func describedSchedule(s Schedule) *client.ScheduleDescription {
	fingerprint := scheduleFingerprint(s)
	return &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{
				ID: desiredActionID(s),
				Memo: map[string]interface{}{
					scheduleFingerprintMemoKey: fingerprint,
				},
			},
		},
	}
}

var errTest = &testError{msg: "test error"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
