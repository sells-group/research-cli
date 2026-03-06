package fedsync

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/sells-group/research-cli/internal/temporal/sdk"
)

func TestRunWorkflow_NoDatasets(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectDatasets, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectDatasetsResult{DatasetNames: nil}, nil)

	env.ExecuteWorkflow(RunWorkflow, RunParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result RunResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestRunWorkflow_TwoDatasets(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectDatasets, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectDatasetsResult{DatasetNames: []string{"cbp", "fpds"}}, nil)

	// Mock child workflows (DatasetSyncWorkflow).
	env.OnWorkflow(DatasetSyncWorkflow, mock.Anything, mock.Anything).
		Return(&DatasetSyncResult{RowsSynced: 100}, nil)

	env.ExecuteWorkflow(RunWorkflow, RunParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result RunResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestRunWorkflow_PartialFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectDatasets, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectDatasetsResult{DatasetNames: []string{"cbp", "fpds"}}, nil)

	env.OnWorkflow(DatasetSyncWorkflow, mock.Anything, sdk.SyncItemParams{Name: "cbp"}).
		Return(&DatasetSyncResult{RowsSynced: 100}, nil)
	env.OnWorkflow(DatasetSyncWorkflow, mock.Anything, sdk.SyncItemParams{Name: "fpds"}).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)

	env.ExecuteWorkflow(RunWorkflow, RunParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result RunResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Synced)
	require.Equal(t, 1, result.Failed)
}

func TestDatasetSyncWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).StartSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(&sdk.StartSyncLogResult{SyncID: 42}, nil)
	env.OnActivity((*Activities).SyncDataset, mock.Anything, mock.Anything, mock.Anything).
		Return(&SyncDatasetResult{RowsSynced: 150}, nil)
	env.OnActivity((*Activities).CompleteSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(DatasetSyncWorkflow, DatasetSyncParams{Name: "cbp"})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result DatasetSyncResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(150), result.RowsSynced)
}

func TestDatasetSyncWorkflow_SyncFails(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).StartSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(&sdk.StartSyncLogResult{SyncID: 42}, nil)
	env.OnActivity((*Activities).SyncDataset, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)
	env.OnActivity((*Activities).FailSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(DatasetSyncWorkflow, DatasetSyncParams{Name: "cbp"})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestRunWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectDatasets, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectDatasetsResult{DatasetNames: []string{"cbp"}}, nil)

	env.OnWorkflow(DatasetSyncWorkflow, mock.Anything, mock.Anything).
		Return(&DatasetSyncResult{RowsSynced: 100}, nil)

	env.ExecuteWorkflow(RunWorkflow, RunParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify the handler was registered and
	// progress was tracked correctly.
	result, err := env.QueryWorkflow("fedsync_progress")
	require.NoError(t, err)
	var progress sdk.FanOutProgress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, 1, progress.Total)
	require.Equal(t, 1, progress.Completed)
}
