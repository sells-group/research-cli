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

	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return(
		&mocks.ScheduleHandle{}, nil)

	desired := []Schedule{
		{ID: "test-1", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	result, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{"test-1"}, result.Created)
	require.Empty(t, result.Updated)
	require.Empty(t, result.Deleted)
}

func TestReconcile_UpdateExisting(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)

	// Return one existing schedule.
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "test-1"}, nil)

	mockScheduleClient.On("GetHandle", mock.Anything, "test-1").Return(mockHandle)
	mockHandle.On("Delete", mock.Anything).Return(nil)
	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return(
		&mocks.ScheduleHandle{}, nil)

	desired := []Schedule{
		{ID: "test-1", Cron: "0 3 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	result, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{"test-1"}, result.Updated)
	require.Empty(t, result.Created)
}

func TestReconcile_PruneUnmanaged(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)

	// Return one existing schedule not in desired list.
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

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)

	// One existing schedule to update, plus a new one to create.
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "existing"}, nil)

	desired := []Schedule{
		{ID: "existing", Cron: "0 3 * * *", TaskQueue: "q", Workflow: "wf"},
		{ID: "new-one", Cron: "0 4 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	result, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, []string{"new-one"}, result.Created)
	require.Equal(t, []string{"existing"}, result.Updated)

	// Verify no actual schedule operations were called.
	mockScheduleClient.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
	mockScheduleClient.AssertNotCalled(t, "GetHandle", mock.Anything, mock.Anything)
}

func TestReconcile_ListError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(
		(*mocks.ScheduleListIterator)(nil), errTest)

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
	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return(
		(*mocks.ScheduleHandle)(nil), errTest)

	desired := []Schedule{
		{ID: "fail", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	_, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create fail")
}

func TestReconcile_DeleteForUpdateError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "existing"}, nil)

	mockScheduleClient.On("GetHandle", mock.Anything, "existing").Return(mockHandle)
	mockHandle.On("Delete", mock.Anything).Return(errTest)

	desired := []Schedule{
		{ID: "existing", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	_, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete existing for update")
}

func TestReconcile_RecreateError(t *testing.T) {
	mockClient := &mocks.Client{}
	mockScheduleClient := &mocks.ScheduleClient{}
	mockIter := &mocks.ScheduleListIterator{}
	mockHandle := &mocks.ScheduleHandle{}

	mockClient.On("ScheduleClient").Return(mockScheduleClient)
	mockScheduleClient.On("List", mock.Anything, mock.Anything).Return(mockIter, nil)
	mockIter.On("HasNext").Return(true).Once()
	mockIter.On("HasNext").Return(false)
	mockIter.On("Next").Return(&client.ScheduleListEntry{ID: "existing"}, nil)

	mockScheduleClient.On("GetHandle", mock.Anything, "existing").Return(mockHandle)
	mockHandle.On("Delete", mock.Anything).Return(nil)
	mockScheduleClient.On("Create", mock.Anything, mock.Anything).Return(
		(*mocks.ScheduleHandle)(nil), errTest)

	desired := []Schedule{
		{ID: "existing", Cron: "0 2 * * *", TaskQueue: "q", Workflow: "wf"},
	}

	_, err := Reconcile(context.Background(), mockClient, desired, ReconcileOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "recreate existing")
}

var errTest = &testError{msg: "test error"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

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
