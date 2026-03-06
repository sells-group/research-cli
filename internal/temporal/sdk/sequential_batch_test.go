package sdk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSequentialBatch_Empty(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (int, error) {
		return SequentialBatch(ctx, 0, 10, func(_ workflow.Context, _, _ int) error {
			t.Fatal("should not be called")
			return nil
		})
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var batches int
	require.NoError(t, env.GetWorkflowResult(&batches))
	require.Equal(t, 0, batches)
}

func TestSequentialBatch_ExactBatches(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (int, error) {
		var ranges [][2]int
		batches, err := SequentialBatch(ctx, 6, 3, func(_ workflow.Context, start, end int) error {
			ranges = append(ranges, [2]int{start, end})
			return nil
		})
		if err != nil {
			return 0, err
		}
		if len(ranges) != 2 || ranges[0] != [2]int{0, 3} || ranges[1] != [2]int{3, 6} {
			return 0, fmt.Errorf("unexpected ranges: %v", ranges)
		}
		return batches, nil
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var batches int
	require.NoError(t, env.GetWorkflowResult(&batches))
	require.Equal(t, 2, batches)
}

func TestSequentialBatch_PartialLastBatch(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (int, error) {
		var lastEnd int
		batches, err := SequentialBatch(ctx, 5, 2, func(_ workflow.Context, _, end int) error {
			lastEnd = end
			return nil
		})
		if err != nil {
			return 0, err
		}
		if lastEnd != 5 {
			return 0, fmt.Errorf("last batch end should be 5, got %d", lastEnd)
		}
		return batches, nil
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var batches int
	require.NoError(t, env.GetWorkflowResult(&batches))
	require.Equal(t, 3, batches)
}

func TestSequentialBatch_ErrorStops(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (int, error) {
		callCount := 0
		return SequentialBatch(ctx, 10, 3, func(_ workflow.Context, _, _ int) error {
			callCount++
			if callCount == 2 {
				return fmt.Errorf("batch 2 failed")
			}
			return nil
		})
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestSequentialBatch_DefaultBatchSize(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	wf := func(ctx workflow.Context) (int, error) {
		return SequentialBatch(ctx, 150, 0, func(_ workflow.Context, _, _ int) error {
			return nil
		})
	}

	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var batches int
	require.NoError(t, env.GetWorkflowResult(&batches))
	require.Equal(t, 2, batches) // 150/100 = 1 full + 1 partial
}
