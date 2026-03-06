package sdk

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// stubChild is a minimal child workflow for testing FanOut.
func stubChild(_ workflow.Context, _ SyncItemParams) (*SyncItemResult, error) {
	return &SyncItemResult{RowsSynced: 42}, nil
}

func TestFanOut_EmptyItems(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            nil,
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestFanOut_TwoItems(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 100}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"alpha", "beta"},
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestFanOut_PartialFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, SyncItemParams{Name: "good"}).
		Return(&SyncItemResult{RowsSynced: 50}, nil)
	env.OnWorkflow(stubChild, mock.Anything, SyncItemParams{Name: "bad"}).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"good", "bad"},
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Synced)
	require.Equal(t, 1, result.Failed)
}

func TestFanOut_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 10}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"one"},
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	result, err := env.QueryWorkflow("test_progress")
	require.NoError(t, err)
	var progress FanOutProgress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, 1, progress.Total)
	require.Equal(t, 1, progress.Completed)
}

func TestFanOut_CustomConcurrency(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 1}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"a", "b", "c"},
			Concurrency:      2,
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 3, result.Synced)
}

// customChild is a child workflow that takes a custom param type for testing ChildParamsFn.
func customChild(_ workflow.Context, val int) (*SyncItemResult, error) {
	return &SyncItemResult{RowsSynced: int64(val)}, nil
}

func TestFanOut_ChildParamsFn(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(customChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 99}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"x"},
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
			ChildParamsFn: func(_ string) []interface{} {
				return []interface{}{42}
			},
			ResultHandlerFn: func(item string, gCtx workflow.Context, f workflow.ChildWorkflowFuture) ItemOutcome {
				var result SyncItemResult
				if err := f.Get(gCtx, &result); err != nil {
					return ItemOutcome{Name: item, Status: "failed", Error: err.Error()}
				}
				return ItemOutcome{Name: item, Status: "complete", RowsSynced: result.RowsSynced}
			},
		}, customChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Synced)
}

func TestFanOut_ResultHandlerFn(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 10}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"item1"},
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
			ResultHandlerFn: func(item string, gCtx workflow.Context, f workflow.ChildWorkflowFuture) ItemOutcome {
				var result SyncItemResult
				if err := f.Get(gCtx, &result); err != nil {
					return ItemOutcome{Name: item, Status: "failed", Error: err.Error()}
				}
				// Custom: double the rows for testing.
				return ItemOutcome{Name: item, Status: "complete", RowsSynced: result.RowsSynced * 2}
			},
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Synced)
	require.Equal(t, int64(20), result.Outcomes[0].RowsSynced)
}

func TestFanOut_PauseResumeSignals(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 1}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"a", "b"},
			Concurrency:      5,
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
			PauseSignal:      "pause",
			ResumeSignal:     "resume",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Synced)
}

func TestFanOut_PausedProgress(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnWorkflow(stubChild, mock.Anything, mock.Anything).
		Return(&SyncItemResult{RowsSynced: 1}, nil)

	wf := func(ctx workflow.Context) (*FanOutResult, error) {
		return FanOut(ctx, FanOutParams{
			Items:            []string{"a"},
			Concurrency:      5,
			QueryName:        "test_progress",
			WorkflowIDPrefix: "test",
			PauseSignal:      "pause",
			ResumeSignal:     "resume",
		}, stubChild)
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Verify the progress query includes Paused field (false after completion).
	result, err := env.QueryWorkflow("test_progress")
	require.NoError(t, err)
	var progress FanOutProgress
	require.NoError(t, result.Get(&progress))
	require.False(t, progress.Paused)
	require.Equal(t, 1, progress.Completed)
}
