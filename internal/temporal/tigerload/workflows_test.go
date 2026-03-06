package tigerload

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	states := []StateFIPS{
		{Abbr: "WY", FIPS: "56"},
		{Abbr: "VT", FIPS: "50"},
	}

	env.OnActivity((*Activities).LoadNational, mock.Anything, mock.Anything, mock.Anything).
		Return(&LoadNationalResult{Loaded: 5}, nil)

	env.OnActivity((*Activities).CreateAllStateTables, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.OnActivity((*Activities).ResolveStates, mock.Anything, mock.Anything, mock.Anything).
		Return(&ResolveStatesResult{States: states}, nil)

	env.OnActivity((*Activities).PopulateLookups, mock.Anything, mock.Anything).
		Return(nil)

	env.OnWorkflow(TigerStateWorkflow, mock.Anything, mock.Anything).
		Return(&TigerStateResult{RowsLoaded: 1000}, nil)

	env.ExecuteWorkflow(Workflow, Params{
		Year:        2024,
		Concurrency: 2,
		Incremental: true,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result Result
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 5, result.National)
	require.Equal(t, 2, result.Loaded)
	require.Equal(t, 0, result.Failed)
	require.Len(t, result.Outcomes, 2)
}

func TestWorkflow_NationalFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	// CreateAllStateTables runs first (DDL before COPY).
	env.OnActivity((*Activities).CreateAllStateTables, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.OnActivity((*Activities).LoadNational, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, temporal.NewNonRetryableApplicationError(
			"download failed", "DOWNLOAD_ERROR", nil))

	env.ExecuteWorkflow(Workflow, Params{
		Year:        2024,
		Concurrency: 3,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestWorkflow_StateFailureContinues(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	states := []StateFIPS{
		{Abbr: "WY", FIPS: "56"},
		{Abbr: "VT", FIPS: "50"},
	}

	env.OnActivity((*Activities).LoadNational, mock.Anything, mock.Anything, mock.Anything).
		Return(&LoadNationalResult{Loaded: 5}, nil)

	env.OnActivity((*Activities).CreateAllStateTables, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.OnActivity((*Activities).ResolveStates, mock.Anything, mock.Anything, mock.Anything).
		Return(&ResolveStatesResult{States: states}, nil)

	env.OnActivity((*Activities).PopulateLookups, mock.Anything, mock.Anything).
		Return(nil)

	// First call succeeds, second fails.
	env.OnWorkflow(TigerStateWorkflow, mock.Anything, mock.Anything).
		Return(&TigerStateResult{RowsLoaded: 500}, nil).Once()
	env.OnWorkflow(TigerStateWorkflow, mock.Anything, mock.Anything).
		Return(nil, temporal.NewNonRetryableApplicationError(
			"state load failed", "STATE_ERROR", nil)).Once()

	env.ExecuteWorkflow(Workflow, Params{
		Year:        2024,
		Concurrency: 2,
		Incremental: true,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result Result
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Loaded)
	require.Equal(t, 1, result.Failed)
}

func TestWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	states := []StateFIPS{
		{Abbr: "WY", FIPS: "56"},
	}

	env.OnActivity((*Activities).LoadNational, mock.Anything, mock.Anything, mock.Anything).
		Return(&LoadNationalResult{Loaded: 5}, nil)

	env.OnActivity((*Activities).CreateAllStateTables, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.OnActivity((*Activities).ResolveStates, mock.Anything, mock.Anything, mock.Anything).
		Return(&ResolveStatesResult{States: states}, nil)

	env.OnActivity((*Activities).PopulateLookups, mock.Anything, mock.Anything).
		Return(nil)

	env.OnWorkflow(TigerStateWorkflow, mock.Anything, mock.Anything).
		Return(&TigerStateResult{RowsLoaded: 1000}, nil)

	env.ExecuteWorkflow(Workflow, Params{
		Year:        2024,
		Concurrency: 3,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify handler was registered.
	result, err := env.QueryWorkflow("tiger_load_progress")
	require.NoError(t, err)
	var progress Progress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, 1, progress.TotalStates)
	require.Equal(t, 1, progress.Completed)
}

func TestTigerStateWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).LoadStateProducts, mock.Anything, mock.Anything, mock.Anything).
		Return(&LoadStateProductResult{RowsLoaded: 5000}, nil)

	env.ExecuteWorkflow(TigerStateWorkflow, TigerStateParams{
		State:       "WY",
		FIPS:        "56",
		Year:        2024,
		Products:    []string{"EDGES", "FACES", "ADDR", "FEATNAMES"},
		Incremental: true,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result TigerStateResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(5000), result.RowsLoaded)
}

func TestTigerStateWorkflow_Failure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).LoadStateProducts, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, temporal.NewNonRetryableApplicationError(
			"download failed", "DOWNLOAD_ERROR", nil))

	env.ExecuteWorkflow(TigerStateWorkflow, TigerStateParams{
		State: "WY",
		FIPS:  "56",
		Year:  2024,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestWorkflow_DefaultConcurrency(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	states := []StateFIPS{
		{Abbr: "WY", FIPS: "56"},
	}

	env.OnActivity((*Activities).LoadNational, mock.Anything, mock.Anything, mock.Anything).
		Return(&LoadNationalResult{Loaded: 5}, nil)

	env.OnActivity((*Activities).CreateAllStateTables, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.OnActivity((*Activities).ResolveStates, mock.Anything, mock.Anything, mock.Anything).
		Return(&ResolveStatesResult{States: states}, nil)

	env.OnActivity((*Activities).PopulateLookups, mock.Anything, mock.Anything).
		Return(nil)

	env.OnWorkflow(TigerStateWorkflow, mock.Anything, mock.Anything).
		Return(&TigerStateResult{RowsLoaded: 100}, nil)

	// Concurrency=0 should default to 3.
	env.ExecuteWorkflow(Workflow, Params{
		Year: 2024,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result Result
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Loaded)
}
